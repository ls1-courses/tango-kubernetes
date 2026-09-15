"""Kubernetes VMMS backend for Tango.

Each submission becomes one Kubernetes Job. Input files are staged in a
short-lived Secret; the workload receives neither a ServiceAccount token nor
registry credentials.
"""

import base64
import hashlib
import logging
import os
import posixpath
import re
import stat
import threading
import time
import uuid

from kubernetes import client, config as kube_config
from kubernetes.client.exceptions import ApiException

import config
from tangoObjects import TangoMachine


_IMAGE_RE = re.compile(
    r"^(?:[a-zA-Z0-9.-]+(?::[0-9]+)?/)?"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?"
    r"(?:@sha256:[a-fA-F0-9]{64})?$"
)


class Kubernetes(object):
    def __init__(self, batch_api=None, core_api=None):
        self.log = logging.getLogger("Kubernetes")
        if batch_api is None or core_api is None:
            if config.Config.KUBERNETES_KUBECONFIG:
                kube_config.load_kube_config(
                    config_file=config.Config.KUBERNETES_KUBECONFIG
                )
            else:
                kube_config.load_incluster_config()
        self.batch = batch_api or client.BatchV1Api()
        self.core = core_api or client.CoreV1Api()
        self.namespace = config.Config.KUBERNETES_NAMESPACE
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(
            config.Config.KUBERNETES_MAX_CONCURRENT_JOBS
        )
        self._state = {}

    def instanceName(self, id, name):
        digest = hashlib.sha256(str(name).encode("utf-8")).hexdigest()[:10]
        prefix = re.sub(r"[^a-z0-9-]", "-", config.Config.PREFIX.lower()).strip("-")
        return "%s-%s-%s" % ((prefix or "tango")[:20], id, digest)

    def domainName(self, vm):
        return vm.domain_name

    def initializeVM(self, vm):
        vm.domain_name = "%s-%s" % (
            self.instanceName(vm.id, vm.image),
            uuid.uuid4().hex[:8],
        )
        with self._lock:
            self._state[vm.domain_name] = {}
        return vm

    def waitVM(self, vm, max_secs):
        return 0

    @staticmethod
    def _destination(path):
        path = path.replace("\\", "/")
        if path.startswith("/"):
            raise ValueError("invalid destination path: %s" % path)
        normalized = posixpath.normpath(path)
        if normalized in ("", ".") or normalized.startswith("../"):
            raise ValueError("invalid destination path: %s" % path)
        return normalized

    def copyIn(self, vm, inputFiles):
        self._slots.acquire()
        with self._lock:
            self._state[vm.domain_name]["slot"] = True
        binary_data = {}
        items = []
        total = 0
        for number, input_file in enumerate(inputFiles):
            with open(input_file.localFile, "rb") as source:
                contents = source.read()
            total += len(contents)
            if total > config.Config.KUBERNETES_INPUT_LIMIT:
                raise ValueError(
                    "input bundle is %d bytes; Kubernetes Secret staging limit is %d"
                    % (total, config.Config.KUBERNETES_INPUT_LIMIT)
                )
            key = "file-%04d" % number
            binary_data[key] = base64.b64encode(contents).decode("ascii")
            mode = stat.S_IMODE(os.stat(input_file.localFile).st_mode)
            items.append(
                client.V1KeyToPath(
                    key=key,
                    path=self._destination(input_file.destFile),
                    mode=mode or 0o644,
                )
            )

        secret_name = "%s-input" % vm.domain_name
        secret = client.V1Secret(
            metadata=client.V1ObjectMeta(
                name=secret_name,
                labels={
                    "app.kubernetes.io/managed-by": "tango",
                    "grading.ls1.dev/instance": vm.domain_name,
                },
            ),
            type="Opaque",
            data=binary_data,
        )
        self.core.create_namespaced_secret(self.namespace, secret)
        with self._lock:
            self._state[vm.domain_name]["secret"] = secret_name
            self._state[vm.domain_name]["items"] = items
        return 0

    @staticmethod
    def _container_security():
        return client.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=client.V1Capabilities(drop=["ALL"]),
        )

    def _resources(self, vm):
        storage = config.Config.KUBERNETES_EPHEMERAL_STORAGE
        requests = {"ephemeral-storage": storage}
        limits = {"ephemeral-storage": storage}
        if vm.cores:
            requests["cpu"] = str(vm.cores)
            limits["cpu"] = str(vm.cores)
        if vm.memory:
            requests["memory"] = "%sMi" % vm.memory
            limits["memory"] = "%sMi" % vm.memory
        return client.V1ResourceRequirements(requests=requests, limits=limits)

    def _job(self, vm, run_timeout, max_output_size):
        with self._lock:
            state = self._state[vm.domain_name]
        input_volume = client.V1Volume(
            name="input",
            secret=client.V1SecretVolumeSource(
                secret_name=state["secret"], items=state["items"]
            ),
        )
        work_volume = client.V1Volume(
            name="work", empty_dir=client.V1EmptyDirVolumeSource()
        )
        init = client.V1Container(
            name="prepare",
            image=vm.image,
            command=["sh", "-c"],
            args=[
                "cp -a /home/autolab/. /workspace/ 2>/dev/null || true; "
                "cp -a /input/. /workspace/"
            ],
            resources=self._resources(vm),
            security_context=self._container_security(),
            volume_mounts=[
                client.V1VolumeMount(name="input", mount_path="/input", read_only=True),
                client.V1VolumeMount(name="work", mount_path="/workspace"),
            ],
        )
        command = "cd /home && exec autodriver -u %d -f %d -t %d -o %d autolab" % (
            config.Config.VM_ULIMIT_USER_PROC,
            config.Config.VM_ULIMIT_FILE_SIZE,
            run_timeout,
            max_output_size,
        )
        grader = client.V1Container(
            name="grader",
            image=vm.image,
            command=["sh", "-c"],
            args=[command],
            resources=self._resources(vm),
            security_context=self._container_security(),
            volume_mounts=[
                client.V1VolumeMount(name="work", mount_path="/home/autolab")
            ],
        )
        uid = config.Config.KUBERNETES_RUN_AS_USER
        pod_security = client.V1PodSecurityContext(
            run_as_non_root=True,
            run_as_user=uid,
            run_as_group=uid,
            fs_group=uid,
            seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
        )
        pod_spec = client.V1PodSpec(
            active_deadline_seconds=max(1, run_timeout),
            automount_service_account_token=False,
            containers=[grader],
            init_containers=[init],
            node_selector={
                config.Config.KUBERNETES_NODE_SELECTOR_KEY: config.Config.KUBERNETES_NODE_SELECTOR_VALUE
            },
            restart_policy="Never",
            runtime_class_name=config.Config.KUBERNETES_RUNTIME_CLASS,
            security_context=pod_security,
            service_account_name=config.Config.KUBERNETES_SERVICE_ACCOUNT,
            tolerations=[
                client.V1Toleration(
                    key="grading.ls1.dev/dedicated",
                    operator="Equal",
                    value="true",
                    effect="NoSchedule",
                )
            ],
            volumes=[input_volume, work_volume],
        )
        labels = {
            "app.kubernetes.io/managed-by": "tango",
            "app.kubernetes.io/name": "grading-job",
            "grading.ls1.dev/instance": vm.domain_name,
        }
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels), spec=pod_spec
        )
        spec = client.V1JobSpec(
            active_deadline_seconds=max(1, run_timeout),
            backoff_limit=0,
            template=template,
            ttl_seconds_after_finished=300,
        )
        return client.V1Job(
            metadata=client.V1ObjectMeta(name=vm.domain_name, labels=labels),
            spec=spec,
        )

    def _pod_name(self, vm):
        pods = self.core.list_namespaced_pod(
            self.namespace, label_selector="job-name=%s" % vm.domain_name
        ).items
        return pods[0].metadata.name if pods else None

    def runJob(self, vm, runTimeout, maxOutputFileSize, disableNetwork):
        # The grading namespace is default-deny regardless of the legacy flag.
        self.batch.create_namespaced_job(
            self.namespace, self._job(vm, runTimeout, maxOutputFileSize)
        )
        deadline = time.monotonic() + runTimeout + 60
        while time.monotonic() < deadline:
            current = self.batch.read_namespaced_job_status(
                vm.domain_name, self.namespace
            )
            if current.status.succeeded:
                return 0
            if current.status.failed:
                conditions = current.status.conditions or []
                if any(c.reason == "DeadlineExceeded" for c in conditions):
                    return 2
                pod_name = self._pod_name(vm)
                if pod_name:
                    pod = self.core.read_namespaced_pod_status(pod_name, self.namespace)
                    statuses = pod.status.container_statuses or []
                    if statuses and statuses[0].state.terminated:
                        code = statuses[0].state.terminated.exit_code
                        return code if code in (1, 2, 3) else 3
                return 3
            time.sleep(config.Config.TIMER_POLL_INTERVAL)
        return 2

    def _logs(self, vm):
        pod_name = self._pod_name(vm)
        if not pod_name:
            return ""
        return self.core.read_namespaced_pod_log(
            pod_name, self.namespace, container="grader", timestamps=False
        )

    def copyOut(self, vm, destFile):
        output = self._logs(vm).encode("utf-8", errors="replace")
        with open(destFile, "wb") as destination:
            destination.write(output[: config.Config.MAX_OUTPUT_FILE_SIZE])
        return 0

    def destroyVM(self, vm):
        name = vm.domain_name or self.instanceName(vm.id, vm.image)
        try:
            self.batch.delete_namespaced_job(
                name, self.namespace, propagation_policy="Background"
            )
        except ApiException as error:
            if error.status != 404:
                raise
        try:
            self.core.delete_namespaced_secret("%s-input" % name, self.namespace)
        except ApiException as error:
            if error.status != 404:
                raise
        with self._lock:
            state = self._state.pop(name, None)
        if state and state.get("slot"):
            self._slots.release()

    def safeDestroyVM(self, vm):
        self.destroyVM(vm)

    def existsVM(self, vm):
        try:
            self.batch.read_namespaced_job(vm.domain_name, self.namespace)
            return True
        except ApiException as error:
            if error.status == 404:
                return False
            raise

    def getVMs(self):
        jobs = self.batch.list_namespaced_job(
            self.namespace,
            label_selector="app.kubernetes.io/managed-by=tango",
        ).items
        return [
            TangoMachine(
                name=job.metadata.name,
                vmms="kubernetes",
                domain_name=job.metadata.name,
            )
            for job in jobs
        ]

    def getImages(self):
        # OCI registries are intentionally not enumerable by the controller.
        return []

    def imageAvailable(self, image):
        # The kubelet verifies availability and digest/tag resolution on pull.
        return bool(image and _IMAGE_RE.fullmatch(image))

    def getPartialOutput(self, vm):
        return self._logs(vm)[: config.Config.MAX_OUTPUT_FILE_SIZE]
