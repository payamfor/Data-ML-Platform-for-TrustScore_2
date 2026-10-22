"""Proof that the client's feature logic was not changed by the move to EMR.

The hashes below were taken from the ORIGINAL files the SageMaker team ran:

* ``sagemaker/trust_score_05/features/*.py`` - the v5 feature library
* ``sagemaker/feature_engineering.py``       - the notebook driver

Each library file here must hash to its original once the package rename
(``trust_score_05.features`` -> ``ts05_features.library``) is undone. In the
driver, the whole notebook region and every function and setting not listed in
``ADAPTED`` or ``APPROVED_EMR_ADAPTATIONS_SHA256`` must hash to the original's
source text exactly. Approved EMR-specific deviations have their own pinned
hashes.

If one of these tests fails, the feature logic has drifted from the client's
code. Do not update a hash to make it pass without the DS team's sign-off.
"""

from __future__ import annotations

import ast
import hashlib
import os
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parent.parent / "ts05_features"
LIBRARY = PACKAGE / "library"
DRIVER = PACKAGE / "feature_engineering.py"

ORIGINAL_LIBRARY_SHA256 = {
    "__init__.py": "482e4064f9c079f71bc13de0f0ef052cd2d3e8f7da235ab1ed890c37a4c23733",
    "assemble.py": "bdf6f7a1dd87fb28053c7c56eb27f68dd665c2e7ecbfb824880f04bd5e26f80f",
    "calculators.py": "a3800d989f8fc7c4307d136788fc701e7e904f639fc1a5a46245fa50265592f7",
    "config.py": "ebb758296b95149e4617079ae3d772e9b5c518ab984e4630171e3fc70912800e",
    "enstream.py": "923a20106dc3cc16e595807f145cebe2e2c1772bfcd874f3d6ffb45f495a6a37",
    "events.py": "fa1797e7fab9ea5880406bece09fca8abd18f0977c2664d537827e7b3f5f00ee",
    "expressions.py": "768516768480bf20971a35d4b75836baeeb5a49f46113d5e56fc6f85b6d8c109",
    "snapshots.py": "69141e24446afe3808808f384309ddd004b1f23761ed1a580a91b8079411a987",
}

#: From "# Verbatim from the notebook below this line, unless marked." through
#: the module-level ``GENERATED_WINDOWS = build_biweekly_windows(...)``.
ORIGINAL_NOTEBOOK_REGION_SHA256 = "2dd2729678f93b8c6bbfe71bb9a94bf6717f03ec1c8d74f90636f1b17e254d36"

#: Top-level definitions of the original driver, by source-text hash.
ORIGINAL_DEFINITIONS_SHA256 = {
    "ENABLE_ROW_COUNTS": "5847ccaa842fba9d0536256227bb295ab6a3ffc5fcfb16901a8d0ce813b9e64f",
    "DEBUG_SHOW": "19b4e7de723a362aaf117c8fe14d15f97dcdda5f5cc50a9b5ab6f2aaeddb53c2",
    "OUTPUT_VERSION": "4ae30e40fb7804284c75492484520f83e3a20564ce618e9f1d5f1e376c4f6950",
    "OUTPUT_WRITE_MODE": "dba7a3108fce37be568c2a403481acc88eed86acab243c329b5f357e5c417598",
    "FORCE_OVERWRITE": "79d25c7509ae70accceb50ff62e25642748c126a61df1f29d5626785539cd72d",
    "RUN_ENSTREAM_ONLY": "b3f317fc44f6e2bc38029373f9a173f325eb22cf3ee4477cf0a00379889cd5cd",
    "ENSTREAM_ONLY_GROUPS": "fcbdeed67d5a4584213656f1e1d33fed576551b128dbb1d627c4c498d96f1484",
    "REUSE_SCOPED_EVENTS": "1983ce4f05025b414c67d3ec01b83a8f9989bd94411b249b3aa1bfccba0528dc",
    "FAIL_FAST": "5ebeb9152257154f7fbe7fce5e341162a0a1146e4dd4bdfc7cc28d6281738a23",
    "OUTPUT_PARTITIONS": "cdf50d1d30697eb7f7d49b840f38ed78baf81db62f37130401cfc54f2d314610",
    "MIN_OUTPUT_PARTITIONS": "2f316f452a33c62aeece337348d06e0e0f2ef92927ac0c97cd9a382f7e763cf7",
    "TARGET_OUTPUT_FILE_BYTES": "c62096057deb7762eebf66fb9c91b29c0f7a2c18b237964988b6152e59cd45ff",
    "ASSUMED_BYTES_PER_VALUE": "f6fd8e2dc00406087cdd7027f76458eb92bc9721a1325d146ac3f67e8904d430",
    "ASSUMED_BYTES_PER_STRING": "57f1ff4b88d7dcfae176fd201ce9474033ac4713482df527d1c76df040d103b4",
    "ASSUMED_PARQUET_COMPRESSION": "ba1ee63b5aa889e5086968d8aa8d2a899a4fc089277fe71dfd0c7712f5ea2b3e",
    "OUTPUT_WRITE_FORMAT": "79abf6c8002c7c5490c64954ea2ecb71c7274da30db7c00fdf1da1efc8e18f58",
    "SAMPLE_ENABLED": "42a34467192a36c7d291bb33724f57ada92ca56d75566d4e293db31e39fe0e33",
    "SAMPLE_FRACTION": "df14c4da1b685a3cb5eefb0ab7fbe3368d7b221e5ff2065c7edb546f49576950",
    "SAMPLE_BUCKETS": "e8d2026ccd9067c445f5965f5fad5a9025f43f5cbbb69ea372f786c286e70b19",
    "SAMPLE_SALT": "ebe1b3b0a652d995dac728ddda605a9e93854f5731d680f307c2a2274486de30",
    "SAMPLE_PER_WINDOW": "e10e1dab57946bdfca012984d20812995b42f640ed334adb0c1c8af0a176deaa",
    "SAMPLE_VERIFY": "572e4f5830e03395521221991153beb95d64aca0865fab7146a7e335c78e0145",
    "SAMPLE_TAG": "35666d6c06a0ebce86d0301797016d3f910ef5d0e5e23f6afd90dee5e4c1899b",
    "_sample_verified": "0059cb8e442f6c8b07f0216e74dc463c788236f73203af156d070f6c30bef71c",
    "RESUME_RUN_ID": "199310bc0b3d4f057a2b4d56fc67a63fc638ac8744937c15a268e7f675a4b8ce",
    "MAX_WINDOWS_PER_RUN": "08930875cbaf45812fce55f741d5602a1a99692abe80ea9de5488bf809415afc",
    "RUN_ID": "be8f77118b4c4539a6f097360cb67465c4127e4465d4392ef2544d691b337ae9",
    "ATTEMPT_ID": "bb8138341fb6f022edc06593cf4005329d1e89bd281e304b76f3ba6801db9095",
    "MANIFEST_BASE_PATH": "9f23832b2dd76242dd39ab944ce762d1b1256ce00c4dcde536b0006deeb7c987",
    "COMPLETED_GROUPS": "0c50ab3aff28d3ffac8880848266005cd8f4aa02798990b3c9600b714afe22eb",
    "fcfg": "c3ae025e0e63158b906dd3a540e1d124f51db9b0d8f3f576b56749c5b88944f1",
    "spark": "05d2a93b28257270c1e78777203a6288c67350770945670c93f66e5f7700472e",
    "log": "14e249cdad074ccb7170ce9c1f7855546bbe97e43b6fd991f6fb9ad95f1594c2",
    "parse_ts": "7a7feafdf3b21bba12e5d36fa61a309d2bf3d15783e0c9c09054a357aa1668d1",
    "fmt_ts": "3779626f2c3b1cac8039708849a33db9c32bca6341304bf81dd148c76bf51aa5",
    "build_biweekly_windows": "7a33cc38ae030d5936b0930c51100eb5550c8e9ee59aec2b74e0942d5f40f3f2",
    "validate_windows": "968002de32ecab30c6577c1de9c119a9df686766b36e29391b334bb32101ab35",
    "materialize": "d649dff95a531c03a10bd12014a512523109405df76f6db6ddc9ba2efe4c6f76",
    "build_and_materialize_changes": "30818c26c067d116f6de086699140c411d2fa1ab227acc9dec5cb9859e68e9fd",
    "safe_count": "698086d523b77dcdbdd50b9d38b55269e7e4a066375bdc98c293a9c0ebcc55c4",
    "output_partitions_for": "15ab2f5c74e47149949569736a343dd1b32f5a72c26d4fe9526db52af5b94770",
    "split_feature_set_name": "b9fb44fbfd8ed8fe2b54af06d00f322f4c2889e082d4b020c5cc6eb6c26f297b",
    "fmt_partition_ts": "d8084bdb4e2bbc67799bbdf8d72f716df63df1d25a88695108a08e8fe86bb041",
    "_path_exists": "3f38c4da226107b48ba95db1919dc1145464526942864f9f2a6c5c7e726ce722",
    "_scratch_table_path": "7aac58bc4987510899555243235603e96ba44c046d037149c0964d59c2dc13dd",
    "expected_feature_groups": "d09cc9fe612d64cf700be4dc13ab40e1c8cc4f345a0088c76cad3808356d137d",
    "load_completed_groups": "d05587a7c1372e4fb2a0a744ebf85672458c9e8c08a79b92428906ed65f0aa7f",
    "log_resume_plan": "4afd8e3ba48b73c51aa21b2c1b9ce4a815b25aaa77efae1d9aaa518b68032fcc",
    "split_feature_output_path": "3f349cf1c0a1fd350c872ede9869884450cfa285d17c75f13fba94264f58df01",
    "create_manifest_df": "22249167be300b0991dab82e93d04bd4fa72a4d02dc2fa38024de07f6bfa4016",
    "save_manifest": "b376e319e6174868add8ada1130124277753054c16939297d0bf47ca0a625d1b",
    "write_feature_group_output_logged": "99485bfe15e26c0479812cfe5b2dfe65f58192b610c80ea7a0a11ea2a5e11cc0",
    "process_one_window": "4c9ee482df34fdb6ac2e008f8fea383ce04866fc15547dd6a0f07c76caf3788e",
    "_cohort_predicate": "e57bdbb0255020b986b12acb552a0578483c2706aec54b48f64d0c7fdcae990f",
    "sample_customer_cohort": "a8c50809df96874fbd07a4654d74b995a865bbac2a5975e4918581d6fd189ffb",
    "GENERATED_WINDOWS": "9c0d8dd7cc9d52b3c11319d64ca516703617246fb065e95258afd117ed3d09c8",
    "NOTEBOOK_SPARK_CONF": "802da2413a49d90a04ff315d87f073cacee01654a05eb55d389fb942e3b527ac",
    "read_source": "8ccba03ea04bde7d61eb63adba0c21904cb0c4cb89635feddeeb5ab4bba4cc04",
    "read_inputs": "b5c43812dfc35f9c3d8efe70b0af8e5c613886c7e2f580d5d9767a34424528ee",
    "run_extraction": "d974e46c5b333e7b1e57bd37e6173951153e336e6eba23b638f44c6c264fa5f5",
    "_GROUP_OK": "490fb45d036f185dc43283f446159a2012a7d53619ceefd8f9e449a31a9052ed",
    "time_window_label": "ac50eec4c5fa7e8b3923f0bddef6f569178028f1cfde195214a6de1739ba8376",
    "window_groups_complete": "04c18c6d405708781d9e63cd1b8076dd0743766c747fb293e6077ec8911daf51",
    "assemble_wide": "b1cce42cc255c3714600cdb992f469cd7f9c06f258b110ea52e04103c59b07bd",
    "write_wide": "17109256d411d85d9024a4c80c2f896c79a1edf3c6f5e0a1bbf748bdab52caf4",
    "read_fraud_labels": "8b3a484c7373306de34a89427d260e58821a89da1276949a68aa184749b47832",
}

# Approved EMR-specific deviations from the original notebook definitions.
APPROVED_EMR_ADAPTATIONS_SHA256 = {
    "materialize": "3246b4ce9bfbc974d3ad5027d90c0974bea2a121ba88e60857ceae9c4c96fb5e",
}

#: The original's SageMaker-specific definitions, deliberately replaced for EMR.
#: Every entry is listed with its reason in CHANGES.md.
ADAPTED = {
    "_PROGRAM_DIR",            # removed: --py-files puts the package on the path
    "_cli",                    # --config repeatable (base.yaml + sandbox.yaml)
    "_load_params",            # YAML/JSON + validation, exit 2 on bad config
    "ARGS", "PARAMS",          # PARAMS now takes the mode (for validation)
    "FEATURES_ROOT",           # replaced by OUTPUT_ROOT / WORK_ROOT / FEATURE_STORE_ROOT
    "SCRATCH_BASE_PATH",       # under WORK_ROOT
    "GOLD_FEATURE_BASE_PATH",  # under FEATURE_STORE_ROOT
    "build_spark",             # + switch off per-directory _SUCCESS
    "ML_ROOT",                 # replaced by ts05_features.layout
    "write_fraud_rows",        # output path only
    "publish",                 # output paths + train/test routing
    "main",                    # summary to S3 + one completion marker, last
}


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _top_level_definitions(source: str):
    out = {}
    lines = source.splitlines()
    for node in ast.parse(source).body:
        name = None
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            name = node.name
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 \
                and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            name = node.target.id
        if name:
            segment = ast.get_source_segment(source, node)
            end_line = lines[node.end_lineno - 1]
            end_byte = node.end_col_offset
            end_char = len(end_line.encode("utf-8")[:end_byte].decode("utf-8"))
            trailing = end_line[end_char:]
            if name in APPROVED_EMR_ADAPTATIONS_SHA256 and trailing.lstrip().startswith("#"):
                segment += trailing
            out[name] = segment
    return out


@pytest.mark.parametrize("filename", sorted(ORIGINAL_LIBRARY_SHA256))
def test_library_file_is_the_original_apart_from_the_package_name(filename):
    data = (LIBRARY / filename).read_bytes()
    restored = data.replace(b"ts05_features.library", b"trust_score_05.features")
    assert hashlib.sha256(restored).hexdigest() == ORIGINAL_LIBRARY_SHA256[filename], (
        f"library/{filename} differs from the original v5 file in more than the package name"
    )


def test_library_has_no_extra_modules():
    assert sorted(p.name for p in LIBRARY.glob("*.py")) == sorted(ORIGINAL_LIBRARY_SHA256)


def test_notebook_region_is_byte_identical():
    source = DRIVER.read_text(encoding="utf-8")
    start = source.index("# Verbatim from the notebook below this line, unless marked.")
    end = source.index("GENERATED_WINDOWS = build_biweekly_windows(")
    end = source.index("\n)\n", end) + 3
    region = source[start:end]
    region = region.replace(
        "    # An empty output may have no Parquet part files from which Spark can infer\n"
        "    # a schema, so use the source frame's schema for the materialized read.\n"
        "    return spark.read.schema(df.schema).parquet(path)  # noqa: F821  (spark is notebook-global)",
        "    return spark.read.parquet(path)  # noqa: F821  (spark is notebook-global)",
        1,
    )
    assert _sha(region) == ORIGINAL_NOTEBOOK_REGION_SHA256


@pytest.mark.parametrize("name", sorted(ORIGINAL_DEFINITIONS_SHA256))
def test_driver_definition_is_unchanged(name):
    assert name not in ADAPTED
    definitions = _top_level_definitions(DRIVER.read_text(encoding="utf-8"))
    assert name in definitions, f"{name} is missing from the driver"
    expected_hash = APPROVED_EMR_ADAPTATIONS_SHA256.get(
        name, ORIGINAL_DEFINITIONS_SHA256[name]
    )
    assert _sha(definitions[name]) == expected_hash, (
        f"{name} differs from its original or approved EMR source"
    )


@pytest.mark.skipif(not os.environ.get("TS05_ORIGINAL_SAGEMAKER_DIR"),
                    reason="set TS05_ORIGINAL_SAGEMAKER_DIR to compare with the original files directly")
def test_against_original_files_on_disk():
    """Byte comparison with a checkout of the original ``sagemaker/`` folder."""
    root = Path(os.environ["TS05_ORIGINAL_SAGEMAKER_DIR"])
    for filename in ORIGINAL_LIBRARY_SHA256:
        original = (root / "trust_score_05" / "features" / filename).read_bytes()
        ours = (LIBRARY / filename).read_bytes()
        assert ours == original.replace(b"trust_score_05.features", b"ts05_features.library"), filename
