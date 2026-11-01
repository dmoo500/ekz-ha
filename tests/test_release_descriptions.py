"""Execute workflow scripts with mocked GitHub APIs."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_release_job_has_permissions_for_release_notes_and_publication():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    permissions = workflow["jobs"]["release"]["permissions"]
    assert permissions["contents"] == "write"
    assert permissions.get("pull-requests") == "read"


def execute_script(script, setup):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to exercise GitHub Actions scripts.")
    harness = (
        "const fs = require('node:fs'); const files = {}; "
        "fs.writeFileSync = (path, data) => { files[path] = data; }; "
        + setup
        + "\n(async () => {\n"
        + script
        + "\n console.log(JSON.stringify({files, calls, errors}));"
        + "\n})().catch(error => {console.log(JSON.stringify({error: error.message, calls, errors}));});"
    )
    result = subprocess.run([node, "-e", harness], text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


@pytest.mark.parametrize(
    ("body", "valid"),
    [
        (
            "## Release note\n\n<!-- Write for users. -->\n\nHistory resumes safely.\n\n## How was this checked?\npytest\n",
            True,
        ),
        ("## Release note\r\n\r\nA better upgrade.\r\n\r\n## Checklist\r\n", True),
        ("## Release note\n<!-- Summary -->\n\n## Checklist\n", False),
        ("No release note", False),
    ],
)
def test_beta_notes_include_only_user_summary(body, valid):
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    step = next(
        step
        for step in workflow["jobs"]["release"]["steps"]
        if step.get("name") == "Write release notes"
    )
    pr = {
        "state": "open",
        "head": {"sha": "commit"},
        "body": body,
        "number": 37,
        "html_url": "https://example.test/pr/37",
    }
    setup = (
        "const calls = []; const errors = []; "
        "process.env.IS_PRERELEASE = 'true'; "
        "const context = {repo:{owner:'owner',repo:'repo'},sha:'commit'}; "
        "const github = {rest:{repos:{listPullRequestsAssociatedWithCommit:()=>{}}},"
        f"paginate:async()=>{json.dumps([pr])}}};"
    )
    result = execute_script(step["with"]["script"], setup)
    if valid:
        notes = result["files"]["RELEASE_NOTES.md"]
        assert "upgrade" in notes or "History resumes safely" in notes
        assert "pytest" not in notes
        assert "<!--" not in notes
    else:
        assert "Add a user-facing Release note" in result["error"]


def test_stable_notes_use_native_label_configuration():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    step = next(
        step
        for step in workflow["jobs"]["release"]["steps"]
        if step.get("name") == "Write release notes"
    )
    setup = (
        "const calls=[]; const errors=[]; process.env.IS_PRERELEASE='false'; "
        "const context={repo:{owner:'owner',repo:'repo'},sha:'commit',ref:'refs/tags/0.3.0'}; "
        "const github={rest:{repos:{generateReleaseNotes:async(args)=>{calls.push(args);"
        "return {data:{body:'Grouped user-facing changes'}};}}}};"
    )
    result = execute_script(step["with"]["script"], setup)
    assert result["files"]["RELEASE_NOTES.md"] == "Grouped user-facing changes"
    assert result["calls"][0]["configuration_file_path"] == ".github/release.yml"
    assert result["calls"][0]["tag_name"] == "0.3.0"


@pytest.mark.parametrize("selection", ["bugfix", "enhancement", "missing", "multiple"])
def test_category_labels_are_replaced_without_removing_area_labels(selection):
    workflow = yaml.safe_load((ROOT / ".github/workflows/labeler.yml").read_text())
    step = next(
        step
        for step in workflow["jobs"]["label-pr"]["steps"]
        if step.get("name") == "Label based on PR content"
    )
    body = "" if selection == "missing" else f"- [x] Bug fix — `type: {selection}`"
    if selection == "multiple":
        body = "- [x] Bug fix — `type: bugfix`\n- [X] Improvement — `type: enhancement`"
    setup = (
        "const calls=[]; const errors=[]; const core={setFailed:message=>errors.push(message)}; "
        "const context={repo:{owner:'owner',repo:'repo'},payload:{pull_request:"
        + json.dumps(
            {"number": 37, "body": body, "title": "Improvement", "additions": 10, "deletions": 5}
        )
        + "}}; const github={paginate:async()=>[{name:'type: feature'},{name:'size/XL'},{name:'statistics'}],"
        "rest:{issues:{listLabelsOnIssue:()=>{}, removeLabel:async(args)=>calls.push(['remove',args.name]),"
        "getLabel:async()=>{throw Object.assign(new Error('missing'),{status:404});},"
        "createLabel:async(args)=>calls.push(['create',args.name]),"
        "addLabels:async(args)=>calls.push(['add',args.labels])}}};"
    )
    result = execute_script(step["with"]["script"], setup)
    assert ["remove", "type: feature"] in result["calls"]
    assert ["remove", "size/XL"] in result["calls"]
    assert ["remove", "statistics"] not in result["calls"]
    if selection in {"bugfix", "enhancement"}:
        assert result["errors"] == []
        assert ["create", f"type: {selection}"] in result["calls"]
    else:
        assert result["errors"]
