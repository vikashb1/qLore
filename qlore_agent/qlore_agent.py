import json
import re

from ollama import chat

from qlore_agent.tools.device_health import get_device_health

from qlore_agent.tools.incidents import (
    get_open_incidents,
    get_device_incident_summary,
    get_critical_incidents,
)

from qlore_agent.tools.query_trino import query_trino


# ============================================================
# CONFIGURATION
# ============================================================

MODEL_NAME = "qwen2.5:1.5b"

MAX_TOOL_ROUNDS = 4


# ============================================================
# SYSTEM PROMPT
# ============================================================

SYSTEM_PROMPT = """
You are qLore, a local operations investigation assistant for
a quantum hardware telemetry platform.

You receive trusted operational evidence from qLore's data systems.

IMPORTANT RULES:

1. Never invent telemetry values.
2. Never invent incident counts.
3. Never invent incident types.
4. Never invent timestamps.
5. Never invent health scores.
6. Never invent SQL tables or columns.
7. Never invent time windows such as "last 24 hours".
8. Do not claim that an incident caused another condition unless
   the evidence explicitly establishes causation.

When evidence has already been retrieved, answer directly.

Do NOT say:
- "I will check"
- "I will investigate"
- "Let's start"
- "I will query"
- "Please wait"

Clearly separate observed evidence from interpretation.

Keep answers concise, technical, and useful to an engineer.
"""


# ============================================================
# BASIC HELPERS
# ============================================================

def json_string(value):
    """
    Convert Python objects to formatted JSON safely.
    """

    return json.dumps(
        value,
        default=str,
        indent=2,
    )


def extract_device_id(text):
    """
    Extract IDs such as DEV001 or dev002.
    """

    match = re.search(
        r"\bDEV\d{3}\b",
        text,
        flags=re.IGNORECASE,
    )

    if not match:
        return None

    return match.group(0).upper()


# ============================================================
# INTENT ROUTING
# ============================================================

def is_device_investigation(question):
    """
    Determine whether the user wants an investigation
    of a specific device.
    """

    device_id = extract_device_id(question)

    if not device_id:
        return False

    text = question.lower()

    keywords = [
        "why",
        "degraded",
        "degrade",
        "wrong",
        "problem",
        "issue",
        "unhealthy",
        "investigate",
        "failure",
        "failing",
        "anomaly",
        "anomalies",
    ]

    return any(
        keyword in text
        for keyword in keywords
    )


def is_critical_incident_question(question):
    """
    Detect fleet-level critical incident questions.
    """

    text = question.lower()

    critical_terms = [
        "critical incident",
        "critical incidents",
        "critical issue",
        "critical issues",
    ]

    return any(
        term in text
        for term in critical_terms
    )


def is_queue_ranking_question(question):
    """
    Detect questions asking which device has the
    highest or lowest queue depth.
    """

    text = question.lower()

    queue_present = (
        "queue" in text
        and "depth" in text
    )

    ranking_present = any(
        term in text
        for term in [
            "highest",
            "lowest",
            "largest",
            "smallest",
            "most",
            "least",
            "max",
            "maximum",
            "min",
            "minimum",
        ]
    )

    return (
        queue_present
        and ranking_present
    )


# ============================================================
# DETERMINISTIC DEVICE INVESTIGATION
# ============================================================

def investigate_device(question, device_id):

    print()
    print(
        f"[qLore] Investigating {device_id}..."
    )

    print(
        "[qLore] Reading device health..."
    )

    health = get_device_health(
        device_id
    )

    print(
        "[qLore] Reading incident summary..."
    )

    incident_summary = (
        get_device_incident_summary(
            device_id
        )
    )

    print(
        "[qLore] Reading open incidents..."
    )

    incidents = get_open_incidents(
        device_id
    )

    evidence = {
        "device_id": device_id,
        "device_health": health,
        "incident_summary":
            incident_summary,
        "open_incident_categories":
            incidents,
    }

    prompt = f"""
The user asked:

{question}

qLore has already completed a deterministic investigation.

TRUSTED EVIDENCE:

{json_string(evidence)}

Answer the question using ONLY the evidence above.

Important interpretation rules:

- incident_summary severity totals are aggregate totals.
- Do NOT assign an aggregate severity total to one incident type.
- open_incident_categories contains the per-incident-type counts.
- Use the per-type count when describing a specific incident.
- Do not invent a time window.
- If timestamps are provided, report the actual timestamps or
  describe them as the observed incident window.
- Do not say "last 24 hours" unless the evidence literally says so.
- Do not invent causes.
- Possible explanations must be labeled as interpretation.

Use this structure:

Overall condition:
...

Observed evidence:
...

Interpretation:
...

Recommended next checks:
...

Mention CRITICAL evidence first, followed by HIGH and MEDIUM evidence.
"""

    response = chat(
        model=MODEL_NAME,
        messages=[
            {
                "role": "system",
                "content":
                    SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        options={
            "temperature": 0.0,
        },
    )

    return (
        response.message.content
        or
        "Unable to generate investigation summary."
    )


# ============================================================
# DETERMINISTIC CRITICAL INCIDENT QUERY
# ============================================================

def answer_critical_incidents(question):

    print()
    print(
        "[qLore] Reading critical incidents..."
    )

    incidents = get_critical_incidents()

    if not incidents:
        return (
            "No currently open CRITICAL "
            "incident categories were found."
        )

    prompt = f"""
The user asked:

{question}

qLore retrieved the following trusted CRITICAL incident data:

{json_string(incidents)}

Answer directly using ONLY this data.

Do not invent devices.
Do not invent errors.
Do not ask the user for a device ID.
Do not invent timestamps or time windows.

For each affected device, state:
- device ID
- incident type
- occurrence count
- observed incident window, if available

Keep the answer short.
"""

    response = chat(
        model=MODEL_NAME,
        messages=[
            {
                "role": "system",
                "content":
                    SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        options={
            "temperature": 0.0,
        },
    )

    return (
        response.message.content
        or json_string(incidents)
    )


# ============================================================
# DETERMINISTIC QUEUE RANKING
# ============================================================

def answer_queue_ranking(question):

    text = question.lower()

    lowest_requested = any(
        term in text
        for term in [
            "lowest",
            "smallest",
            "least",
            "minimum",
            "min ",
        ]
    )

    if lowest_requested:
        direction = "ASC"
        ranking_word = "lowest"
    else:
        direction = "DESC"
        ranking_word = "highest"

    # --------------------------------------------------------
    # Approved SQL
    #
    # The LLM does NOT generate this SQL.
    # --------------------------------------------------------

    sql = f"""
SELECT
    device_id,
    COUNT(*) AS event_count,
    ROUND(AVG(queue_depth), 2) AS avg_queue_depth
FROM iceberg.silver.telemetry
GROUP BY device_id
ORDER BY avg_queue_depth {direction}
LIMIT 5
""".strip()

    print()
    print(
        "[qLore] Running approved Trino queue-depth query..."
    )

    result = query_trino(
        sql
    )

    print(
        "[qLore] Trino query complete."
    )

    prompt = f"""
The user asked:

{question}

qLore executed this approved SQL:

{sql}

TRUSTED QUERY RESULT:

{json_string(result)}

Answer directly using ONLY the query result.

Identify the device with the {ranking_word}
average queue depth.

State:
- device ID
- average queue depth
- number of telemetry events if available

Do not invent operational causes.

You may say that a higher queue depth can indicate greater
queued workload or processing pressure, but clearly label
that as interpretation rather than a proven cause.

Keep the answer concise.
"""

    response = chat(
        model=MODEL_NAME,
        messages=[
            {
                "role": "system",
                "content":
                    SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        options={
            "temperature": 0.0,
        },
    )

    return (
        response.message.content
        or json_string(result)
    )


# ============================================================
# OLLAMA TOOL WRAPPERS
# ============================================================

def tool_get_device_health(
    device_id: str,
) -> str:
    """
    Get current qLore health information for a device.

    Args:
        device_id: Device identifier such as DEV002.
    """

    return json_string(
        get_device_health(
            device_id
        )
    )


def tool_get_open_incidents(
    device_id: str,
) -> str:
    """
    Get open incident categories for a device.

    Args:
        device_id: Device identifier such as DEV002.
    """

    return json_string(
        get_open_incidents(
            device_id
        )
    )


def tool_get_device_incident_summary(
    device_id: str,
) -> str:
    """
    Get incident summary information for a device.

    Args:
        device_id: Device identifier such as DEV002.
    """

    return json_string(
        get_device_incident_summary(
            device_id
        )
    )


def tool_get_critical_incidents() -> str:
    """
    Get currently open CRITICAL incidents.
    """

    return json_string(
        get_critical_incidents()
    )


def tool_query_trino(
    sql: str,
) -> str:
    """
    Execute read-only SQL through qLore Trino.

    Args:
        sql: A read-only SELECT query.
    """

    return json_string(
        query_trino(
            sql
        )
    )


TOOLS = [
    tool_get_device_health,
    tool_get_open_incidents,
    tool_get_device_incident_summary,
    tool_get_critical_incidents,
    tool_query_trino,
]


AVAILABLE_FUNCTIONS = {
    "tool_get_device_health":
        tool_get_device_health,

    "tool_get_open_incidents":
        tool_get_open_incidents,

    "tool_get_device_incident_summary":
        tool_get_device_incident_summary,

    "tool_get_critical_incidents":
        tool_get_critical_incidents,

    "tool_query_trino":
        tool_query_trino,
}


# ============================================================
# GENERAL AGENT
# ============================================================

def run_general_agent(question):

    messages = [
        {
            "role": "system",
            "content": SYSTEM_PROMPT,
        },
        {
            "role": "user",
            "content": question,
        },
    ]

    for _ in range(
        MAX_TOOL_ROUNDS
    ):

        response = chat(
            model=MODEL_NAME,
            messages=messages,
            tools=TOOLS,
            options={
                "temperature": 0.0,
            },
        )

        assistant_message = (
            response.message
        )

        messages.append(
            assistant_message
        )

        tool_calls = (
            assistant_message.tool_calls
            or []
        )

        if not tool_calls:

            content = (
                assistant_message.content
                or ""
            ).strip()

            if content:
                return content

            return (
                "qLore could not produce "
                "a grounded answer."
            )

        for tool_call in tool_calls:

            function_name = (
                tool_call.function.name
            )

            arguments = (
                tool_call.function.arguments
                or {}
            )

            function = (
                AVAILABLE_FUNCTIONS.get(
                    function_name
                )
            )

            print()
            print(
                f"[qLore] Tool: "
                f"{function_name}"
            )

            if function is None:

                result = json_string(
                    {
                        "error":
                            "UnknownTool",

                        "message":
                            (
                                "The requested tool "
                                "does not exist."
                            ),
                    }
                )

            else:

                try:

                    result = function(
                        **arguments
                    )

                except Exception as exc:

                    result = json_string(
                        {
                            "error":
                                type(exc).__name__,

                            "message":
                                str(exc),
                        }
                    )

            messages.append(
                {
                    "role": "tool",
                    "tool_name":
                        function_name,
                    "content":
                        result,
                }
            )

    return (
        "qLore reached the maximum tool-call "
        "limit without producing a final answer."
    )


# ============================================================
# MAIN ROUTER
# ============================================================

def run_agent(question):

    # --------------------------------------------------------
    # Route 1:
    # Specific device investigation
    # --------------------------------------------------------

    device_id = extract_device_id(
        question
    )

    if (
        device_id
        and is_device_investigation(
            question
        )
    ):

        return investigate_device(
            question,
            device_id,
        )

    # --------------------------------------------------------
    # Route 2:
    # Fleet critical incidents
    # --------------------------------------------------------

    if is_critical_incident_question(
        question
    ):

        return answer_critical_incidents(
            question
        )

    # --------------------------------------------------------
    # Route 3:
    # Queue ranking
    # --------------------------------------------------------

    if is_queue_ranking_question(
        question
    ):

        return answer_queue_ranking(
            question
        )

    # --------------------------------------------------------
    # Route 4:
    # General local LLM + tools
    # --------------------------------------------------------

    return run_general_agent(
        question
    )


# ============================================================
# CLI
# ============================================================

def run_cli():

    print()
    print("=" * 72)
    print(
        "qLore Local Operations Agent"
    )
    print("=" * 72)

    print()

    print(
        f"Model: {MODEL_NAME}"
    )

    print(
        "Provider: Ollama (local)"
    )

    print(
        "Agent mode: Hybrid deterministic + LLM"
    )

    print()

    print(
        "No paid API usage required."
    )

    print()

    print(
        "Examples:"
    )

    print(
        "  Why is DEV002 degraded?"
    )

    print(
        "  What is wrong with DEV002?"
    )

    print(
        "  Which devices currently have critical incidents?"
    )

    print(
        "  Which device has the highest average queue depth?"
    )

    print()

    print(
        "Type 'exit' or 'quit' to stop."
    )

    print("=" * 72)

    while True:

        try:

            user_input = input(
                "\nqLore> "
            ).strip()

        except (
            KeyboardInterrupt,
            EOFError,
        ):

            print(
                "\nExiting qLore."
            )

            break

        if not user_input:
            continue

        if user_input.lower() in {
            "exit",
            "quit",
        }:

            print(
                "Exiting qLore."
            )

            break

        try:

            answer = run_agent(
                user_input
            )

            print()
            print("qLore:")
            print(answer)

        except Exception as exc:

            print()
            print(
                "qLore encountered an error:"
            )

            print(
                f"{type(exc).__name__}: "
                f"{exc}"
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_cli()