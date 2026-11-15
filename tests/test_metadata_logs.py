import json
import logging
from test_commit import prepare_upload, commit_request

def test_successful_commit_emits_json_without_contents(caplog):
    service, request, allocation, _ = prepare_upload()
    with caplog.at_level(logging.INFO, logger="dcomp_drive.metadata"):
        service.commit_upload(commit_request(request, allocation))
    event = json.loads(caplog.records[-1].message)
    assert event["component"] == "metadata"
    assert event["level"] == "INFO"
    assert event["operation"] == "commit_upload"
    assert event["event"] == "metadata.operation_completed"
    assert "plaintext_sha256" not in event
