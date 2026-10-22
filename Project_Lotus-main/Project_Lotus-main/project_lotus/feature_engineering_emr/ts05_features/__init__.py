"""Trust Score 0.5 feature engineering, packaged to run on EMR (Serverless).

``ts05_features.library``
    The v5 feature library, copied unchanged from
    ``sagemaker/trust_score_05/features`` (only the package name differs).
``ts05_features.feature_engineering``
    The job: the notebook driver copied unchanged from
    ``sagemaker/feature_engineering.py``, with its SageMaker-specific sections
    replaced by the EMR ones (config, output layout, completion marker).
``ts05_features.layout`` / ``ts05_features.params``
    Where the outputs go and how the config is read. No feature logic.

Named ``ts05_features`` rather than ``trust_score_05`` on purpose: the lineage
and ML jobs ship a ``trust_score_05.zip`` that contains an *older* copy of the
feature library, and two packages with the same name on one Python path would
silently resolve to whichever came first.
"""
