import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from tangoObjects import InputFile, TangoMachine
from vmms.kubernetes import Kubernetes


class TestKubernetes(unittest.TestCase):
    def setUp(self):
        self.batch = Mock()
        self.core = Mock()
        self.backend = Kubernetes(batch_api=self.batch, core_api=self.core)
        self.vm = TangoMachine(
            id=42,
            name="ghcr.io/example/task:abc123",
            image="ghcr.io/example/task:abc123",
            vmms="kubernetes",
            cores=2,
            memory=512,
        )
        self.backend.initializeVM(self.vm)

    def test_image_reference_validation(self):
        self.assertTrue(self.backend.imageAvailable("ghcr.io/example/task:abc123"))
        self.assertTrue(
            self.backend.imageAvailable("ghcr.io/example/task@sha256:" + "a" * 64)
        )
        self.assertFalse(self.backend.imageAvailable("https://ghcr.io/example/task"))
        self.assertFalse(self.backend.imageAvailable("ghcr.io/Example/Task:latest"))

    def test_copy_in_creates_secret_and_preserves_destination(self):
        handle, path = tempfile.mkstemp()
        try:
            os.write(handle, b"all:\n\t@echo ok\n")
            os.close(handle)
            self.backend.copyIn(self.vm, [InputFile(path, "Makefile")])
        finally:
            if os.path.exists(path):
                os.unlink(path)

        namespace, secret = self.core.create_namespaced_secret.call_args.args
        self.assertEqual(namespace, "grading")
        self.assertEqual(secret.metadata.name, self.vm.domain_name + "-input")
        self.assertEqual(secret.data["file-0000"], "YWxsOgoJQGVjaG8gb2sK")

    def test_job_has_sandbox_and_no_token(self):
        self.backend._state[self.vm.domain_name] = {
            "secret": self.vm.domain_name + "-input",
            "items": [],
        }
        job = self.backend._job(self.vm, 120, 1024)
        pod = job.spec.template.spec
        self.assertEqual(pod.runtime_class_name, "grading-runsc")
        self.assertFalse(pod.automount_service_account_token)
        self.assertEqual(pod.service_account_name, "grading-job")
        self.assertEqual(pod.active_deadline_seconds, 120)
        self.assertEqual(pod.node_selector["grading.ls1.dev/enabled"], "true")
        self.assertTrue(pod.security_context.run_as_non_root)
        self.assertEqual(pod.containers[0].resources.limits["cpu"], "2")
        self.assertEqual(pod.containers[0].resources.limits["memory"], "512Mi")

    def test_successful_job(self):
        self.backend._state[self.vm.domain_name] = {
            "secret": self.vm.domain_name + "-input",
            "items": [],
        }
        self.batch.read_namespaced_job_status.return_value = SimpleNamespace(
            status=SimpleNamespace(succeeded=1, failed=None)
        )
        self.assertEqual(self.backend.runJob(self.vm, 120, 1024, True), 0)
        self.batch.create_namespaced_job.assert_called_once()

    def test_destination_cannot_escape_mount(self):
        for path in ("../secret", "/etc/passwd", "foo/../../secret"):
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    self.backend._destination(path)


if __name__ == "__main__":
    unittest.main()
