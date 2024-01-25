"""Python Module to scrape github issues."""
import logging
from typing import NamedTuple

from kfp.v2 import compiler, dsl
from kfp.v2.dsl import component


@component(
    base_image="us-central1-docker.pkg.dev/turing-gpt/gpt-project-images/character_ai_dev:latest",
    packages_to_install=[
        "pandas",
        "pandas-gbq",
        "google-cloud-aiplatform",
        "kfp==2.0.1",
        "tqdm",
        "protobuf==3.19.6",
        "google-cloud-bigquery",
    ],
)
def get_all_contributors(project_id: str) -> NamedTuple("output", [("ret1", str)]):
    """GE component for VAI pipeline."""
    import json  # noqa: F401
    import logging  # noqa: F401
    import re  # noqa: F401
    import time  # noqa: F401
    import warnings  # noqa: F401
    from datetime import datetime  # noqa: F401

    import pandas as pd  # noqa: F401
    from data_pipelines.src_files.contributor import load_contributors  # noqa: F401

    warnings.filterwarnings("ignore")
    load_contributors(project_id=project_id)

    ret1 = "true"
    return (ret1,)

@component(
    base_image="us-central1-docker.pkg.dev/turing-gpt/gpt-project-images/character_ai_dev:latest",
    packages_to_install=[
        "pandas",
        "pandas-gbq",
        "google-cloud-aiplatform",
        "kfp==2.0.1",
        "tqdm",
        "protobuf==3.19.6",
        "google-cloud-bigquery",
    ],
)
def get_all_conversations(project_id: str) -> NamedTuple("output", [("ret1", str)]):
    """GE component for VAI pipeline."""
    import json  # noqa: F401
    import logging  # noqa: F401
    import re  # noqa: F401
    import time  # noqa: F401
    import warnings  # noqa: F401
    from datetime import datetime  # noqa: F401

    import pandas as pd  # noqa: F401
    from data_pipelines.src_files.conversation  import load_conversations

    warnings.filterwarnings("ignore")
    load_conversations(project_id=project_id)

    ret1 = "true"
    return (ret1,)

@component(
    base_image="us-central1-docker.pkg.dev/turing-gpt/gpt-project-images/character_ai_dev:latest",
    packages_to_install=[
        "pandas",
        "pandas-gbq",
        "google-cloud-aiplatform",
        "kfp==2.0.1",
        "tqdm",
        "protobuf==3.19.6",
        "google-cloud-bigquery",
    ],
)
def get_all_conversations_reviews(project_id: str) -> NamedTuple("output", [("ret1", str)]):
    """GE component for VAI pipeline."""
    import json  # noqa: F401
    import logging  # noqa: F401
    import re  # noqa: F401
    import time  # noqa: F401
    import warnings  # noqa: F401
    from datetime import datetime  # noqa: F401

    import pandas as pd  # noqa: F401
    from data_pipelines.src_files.conversation_reviews  import load_conversations_reviews

    warnings.filterwarnings("ignore")
    load_conversations_reviews(project_id=project_id)

    ret1 = "true"
    return (ret1,)


@dsl.pipeline(name="load-character-ai-pipeline")
def pipeline(project: str = "turing-gpt"):
    """VAI Pipeline json creation."""
    logging.info("Starting pipeline")
    logging.info(f"Inside pipeline: {project}")

    contributors = (
        get_all_contributors(project_id=project )
        .set_cpu_limit("8")
        .set_memory_limit("32G")
        .set_caching_options(True)
    )
    conversations = (
        get_all_conversations(project_id=project )
        .set_cpu_limit("8")
        .set_memory_limit("32G")
        .set_caching_options(True)
    )
    reviews = (
        get_all_conversations_reviews(project_id=project )
        .set_cpu_limit("8")
        .set_memory_limit("32G")
        .set_caching_options(True)
    )

    conversations.after(contributors)
    reviews.after(conversations)


compiler.Compiler().compile(pipeline_func=pipeline, package_path="../load_character_ai_data_pipeline.json")
