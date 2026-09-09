"""Keep shared-layer compatibility validation on the path to publication."""

from pathlib import Path
import re


WORKFLOW = (
    Path(__file__).resolve().parents[2] / ".github/workflows/lambda-layer-publish.yml"
)


def jobs() -> dict[str, str]:
    text = WORKFLOW.read_text().split("jobs:\n", 1)[1]
    headers = list(re.finditer(r"^  ([a-z-]+):\n", text, re.MULTILINE))
    return {
        header[1]: text[
            header.start() : headers[i + 1].start()
            if i + 1 < len(headers)
            else len(text)
        ]
        for i, header in enumerate(headers)
    }


def test_publication_waits_for_archive_validation() -> None:
    sections = jobs()
    needs = sections["publish-layer"].split("strategy:", 1)[0]
    assert "- validate-layer" in needs
    assert "needs: build-layer" in sections["validate-layer"]
    assert "scripts/validate_layer.py" in sections["validate-layer"]
    assert "continue-on-error" not in sections["validate-layer"]


def test_pr_validation_cannot_discover_regions_or_publish() -> None:
    sections = jobs()
    for job in ("publish-layer", "resolve-regions"):
        assert "if: github.event_name != 'pull_request'" in sections[job]
    for text in (
        WORKFLOW.read_text().split("jobs:\n", 1)[0],
        sections["build-layer"],
        sections["validate-layer"],
    ):
        assert "id-token: write" not in text


def test_every_advertised_runtime_validates_the_shared_archive() -> None:
    sections = jobs()
    advertised = set(re.findall(r"python(3\.\d+)", sections["publish-layer"]))
    matrix = re.search(r"python-version: \[([^\]]+)\]", sections["validate-layer"])
    assert matrix is not None
    checked = set(re.findall(r'"(3\.\d+)"', matrix[1]))
    assert advertised and advertised <= checked
    builder = re.search(r'python_version: "(3\.\d+)"', sections["build-layer"])
    assert builder is not None
    assert tuple(map(int, builder[1].split("."))) <= min(
        tuple(map(int, value.split("."))) for value in advertised
    )
