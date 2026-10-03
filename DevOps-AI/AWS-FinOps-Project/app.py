import json

import pandas as pd
import streamlit as st

from agent import run_agent
from config import (
    BEDROCK_MODEL_ID,
    BEDROCK_REGION,
)


# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title="AWS FinOps AI Agent",
    page_icon="💰",
    layout="wide",
)


# =========================================================
# HEADER
# =========================================================

st.title("AWS FinOps AI Agent")

st.caption(
    "Agentic AWS cost analysis powered by "
    "Amazon Bedrock + Claude Sonnet"
)


# =========================================================
# SIDEBAR
# =========================================================

with st.sidebar:

    st.header("Architecture")

    st.success(
        "READ-ONLY MODE"
    )

    st.write(
        "**Bedrock Region**"
    )

    st.code(
        BEDROCK_REGION
    )

    st.write(
        "**Bedrock Model / Inference Profile**"
    )

    st.code(
        BEDROCK_MODEL_ID
    )

    st.divider()

    st.write(
        "### Available Agent Tools"
    )

    st.write(
        """
- AWS Cost Explorer
- Regional cost analysis
- Daily cost analysis
- EC2 inventory
- EC2 CloudWatch metrics
- RDS inventory
- RDS CloudWatch metrics
- EBS waste detection
- ECS inventory
"""
    )

    st.divider()

    st.info(
        "The LLM has no AWS credentials. "
        "Python executes explicitly registered "
        "read-only boto3 tools."
    )


# =========================================================
# SESSION STATE
# =========================================================

if "question" not in st.session_state:

    st.session_state.question = (
        "Analyze my AWS environment for cost "
        "optimization opportunities. Investigate "
        "the largest cost drivers, identify "
        "potentially underutilized infrastructure, "
        "look for waste, and give me prioritized "
        "evidence-based recommendations."
    )


# =========================================================
# EXAMPLE QUESTIONS
# =========================================================

st.subheader(
    "Ask the FinOps Agent"
)

col1, col2, col3 = st.columns(3)

if col1.button(
    "Full Cost Analysis",
    use_container_width=True,
):

    st.session_state.question = (
        "Perform a comprehensive AWS cost "
        "optimization analysis. Start with Cost "
        "Explorer, identify the largest cost "
        "drivers, investigate their regions and "
        "resources where tools are available, "
        "and provide prioritized recommendations."
    )


if col2.button(
    "Investigate EC2",
    use_container_width=True,
):

    st.session_state.question = (
        "Investigate my EC2 spending. Determine "
        "which regions are responsible for EC2 "
        "compute cost, inspect running instances "
        "and their CPU utilization, and identify "
        "evidence-backed optimization "
        "opportunities."
    )


if col3.button(
    "Investigate RDS",
    use_container_width=True,
):

    st.session_state.question = (
        "Investigate my Amazon RDS spending. "
        "Determine which regions are responsible "
        "for RDS cost, inspect RDS instances and "
        "their CPU utilization, and identify "
        "potential cost optimization "
        "opportunities."
    )


question = st.text_area(
    "Question",
    value=st.session_state.question,
    height=130,
)


# =========================================================
# RUN ANALYSIS
# =========================================================

if st.button(
    "Run AI FinOps Analysis",
    type="primary",
    use_container_width=True,
):

    status_box = st.status(
        "Claude is investigating AWS...",
        expanded=True,
    )

    progress_messages = []

    def update_progress(message):

        progress_messages.append(
            message
        )

        status_box.write(
            message
        )

    try:

        result = run_agent(
            question,
            progress_callback=
                update_progress,
        )

        status_box.update(
            label=(
                "AWS investigation complete"
            ),
            state="complete",
            expanded=False,
        )

        st.session_state[
            "last_result"
        ] = result

    except Exception as exc:

        status_box.update(
            label="Analysis failed",
            state="error",
        )

        st.exception(exc)


# =========================================================
# RESULTS
# =========================================================

if "last_result" in st.session_state:

    result = st.session_state[
        "last_result"
    ]

    st.divider()

    st.header(
        "AI FinOps Analysis"
    )

    st.markdown(
        result["answer"]
    )

    st.divider()

    col1, col2 = st.columns(2)

    col1.metric(
        "Agent Iterations",
        result["iterations"],
    )

    col2.metric(
        "AWS Tool Calls",
        len(
            result["tool_history"]
        ),
    )


    # =====================================================
    # INVESTIGATION TRACE
    # =====================================================

    st.subheader(
        "Agent Investigation Trace"
    )

    st.caption(
        "This shows which AWS tools Claude "
        "decided to call during the analysis."
    )

    for index, call in enumerate(
        result["tool_history"],
        start=1,
    ):

        with st.expander(
            f"{index}. {call['tool']}"
        ):

            st.write(
                "**Input**"
            )

            st.json(
                call["input"]
            )

            st.write(
                "**AWS Result**"
            )

            st.json(
                call["result"]
            )


# =========================================================
# FOOTER
# =========================================================

st.divider()

st.caption(
    "AWS FinOps AI Agent | "
    "Amazon Bedrock | boto3 | "
    "Claude Sonnet | Streamlit"
)