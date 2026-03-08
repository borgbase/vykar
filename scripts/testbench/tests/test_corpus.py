import os
import random
import tempfile
import unittest
from unittest import mock

from vykar_testbench import corpus


class CorpusTests(unittest.TestCase):
    def test_validate_corpus_mix_accepts_builtin_types(self) -> None:
        corpus.validate_corpus_mix(
            {"mix": [{"type": "bin", "weight": 1, "file_size": "1kb"}]}
        )

    def test_validate_corpus_mix_rejects_unknown_type(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown corpus file type"):
            corpus.validate_corpus_mix(
                {"mix": [{"type": "made-up", "weight": 1, "file_size": "1kb"}]}
            )

    def test_validate_corpus_mix_raises_when_optional_provider_missing(self) -> None:
        with mock.patch.dict(corpus._OPTIONAL_PROVIDERS, {"docx": None}, clear=False):
            with self.assertRaisesRegex(corpus.CorpusDependencyError, "corpus type 'docx' is unavailable"):
                corpus.validate_corpus_mix(
                    {"mix": [{"type": "docx", "weight": 1, "file_size": "1kb"}]}
                )

    def test_validate_corpus_mix_raises_when_optional_probe_fails(self) -> None:
        with mock.patch(
            "vykar_testbench.corpus._probe_optional_type",
            side_effect=corpus.CorpusDependencyError("corpus type 'docx' is unavailable: sample generation failed"),
        ):
            with self.assertRaisesRegex(corpus.CorpusDependencyError, "sample generation failed"):
                corpus.validate_corpus_mix(
                    {"mix": [{"type": "docx", "weight": 1, "file_size": "1kb"}]}
                )

    def test_generate_one_wraps_faker_runtime_failures(self) -> None:
        fake = mock.Mock()
        fake.docx_file.side_effect = FileNotFoundError("default.docx")

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(corpus.CorpusDependencyError, "corpus type 'docx' is unavailable"):
                corpus._generate_one(
                    "docx",
                    tmpdir,
                    1024,
                    {},
                    random.Random(1),
                    "text",
                    fake,
                    [0],
                )

    def test_apply_churn_defaults_to_two_x_growth_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "seed.bin"), "wb") as f:
                f.write(b"x" * 1024)

            stats = corpus.apply_churn(
                tmpdir,
                {"mix": [{"type": "bin", "weight": 1, "file_size": "700b"}]},
                {"add_files": 3, "delete_files": 0, "modify_files": 0, "add_dirs": 0},
                initial_corpus_bytes=1024,
                rng=random.Random(1),
            )

        self.assertEqual(stats["max_allowed_bytes"], 2048)
        self.assertEqual(stats["added"], 1)
        self.assertEqual(stats["skipped_add_files"], 2)

    def test_apply_churn_deletes_before_adding(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            for idx in range(2):
                with open(os.path.join(tmpdir, f"seed-{idx}.bin"), "wb") as f:
                    f.write(b"x" * 1024)

            stats = corpus.apply_churn(
                tmpdir,
                {"mix": [{"type": "bin", "weight": 1, "file_size": "1024b"}]},
                {"add_files": 1, "delete_files": 1, "modify_files": 0, "add_dirs": 0, "max_growth_factor": 1.0},
                initial_corpus_bytes=2048,
                rng=random.Random(2),
            )

        self.assertEqual(stats["deleted"], 1)
        self.assertEqual(stats["added"], 1)
        self.assertEqual(stats["skipped_add_files"], 0)

    def test_apply_churn_skips_directory_when_no_files_fit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "seed.bin"), "wb") as f:
                f.write(b"x" * 1024)

            stats = corpus.apply_churn(
                tmpdir,
                {"mix": [{"type": "bin", "weight": 1, "file_size": "1024b"}]},
                {"add_files": 0, "delete_files": 0, "modify_files": 0, "add_dirs": 1, "max_growth_factor": 1.0},
                initial_corpus_bytes=1024,
                rng=random.Random(3),
            )

            subdirs = [name for name in os.listdir(tmpdir) if os.path.isdir(os.path.join(tmpdir, name))]

        self.assertEqual(stats["dirs_added"], 0)
        self.assertEqual(stats["skipped_add_dirs"], 1)
        self.assertEqual(subdirs, [])

    def test_apply_churn_rejects_growth_factor_below_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "max_growth_factor"):
                corpus.apply_churn(
                    tmpdir,
                    {"mix": [{"type": "bin", "weight": 1, "file_size": "1kb"}]},
                    {"max_growth_factor": 0.5},
                    initial_corpus_bytes=1024,
                    rng=random.Random(4),
                )


if __name__ == "__main__":
    unittest.main()
