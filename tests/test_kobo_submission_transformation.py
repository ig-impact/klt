"""
Unit tests for submission data transformation (EAV conversion).
These tests directly call transform_submission_data() without pipeline overhead.
"""

import json

from klt.resources.kobo_submission import transform_submission_data


def test_transform_separates_metadata_from_questions():
    """Verify metadata fields (_*) stay in root, questions go to responses array."""
    # Arrange
    submission = {
        "_id": 123,
        "_uuid": "abc-def",
        "_submission_time": "2025-11-01T12:00:00Z",
        "_submitted_by": "user1",
        "question1": "answer1",
        "question2": "answer2",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert: Metadata in root
    assert result["_id"] == 123
    assert result["_uuid"] == "abc-def"
    assert result["_submission_time"] == "2025-11-01T12:00:00Z"
    assert result["_submitted_by"] == "user1"

    # Assert: Questions in responses
    assert len(result["responses"]) == 2
    assert {"question": "question1", "response": "answer1"} in result["responses"]
    assert {"question": "question2", "response": "answer2"} in result["responses"]


def test_transform_excludes_geolocation_downloads_validation():
    """Verify excluded fields are completely removed from output."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "_geolocation": [10.0, 20.0],
        "_downloads": ["file1.jpg"],
        "_validation_status": {"valid": True},
        "question1": "answer1",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert: Excluded fields not present
    assert "_geolocation" not in result
    assert "_downloads" not in result
    assert "_validation_status" not in result

    # Assert: Only _id, _uuid, and responses present
    assert result["_id"] == 1
    assert result["_uuid"] == "uuid-a"
    assert len(result["responses"]) == 1


def test_transform_converts_list_responses_to_json():
    """Verify list responses are converted to JSON strings."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "multi_choice": ["option1", "option2", "option3"],
        "single_choice": "option1",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert: List converted to JSON string
    responses = {r["question"]: r["response"] for r in result["responses"]}
    assert responses["multi_choice"] == json.dumps(["option1", "option2", "option3"])

    # Assert: String preserved as-is
    assert responses["single_choice"] == "option1"


def test_transform_preserves_string_responses():
    """Verify simple string responses are preserved unchanged."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "name": "John Doe",
        "email": "john@example.com",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert
    responses = {r["question"]: r["response"] for r in result["responses"]}
    assert responses["name"] == "John Doe"
    assert responses["email"] == "john@example.com"


def test_transform_handles_null_values():
    """Verify null/None values are preserved in responses."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "optional_field": None,
        "filled_field": "data",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert
    responses = {r["question"]: r["response"] for r in result["responses"]}
    assert responses["optional_field"] is None
    assert responses["filled_field"] == "data"


def test_transform_handles_numeric_responses():
    """Verify numeric values (int, float) are preserved as-is."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "age": 25,
        "temperature": 36.5,
        "count": 0,
    }

    # Act
    result = transform_submission_data(submission)

    # Assert
    responses = {r["question"]: r["response"] for r in result["responses"]}
    assert responses["age"] == 25
    assert responses["temperature"] == 36.5
    assert responses["count"] == 0


def test_transform_handles_nested_dict_responses():
    """Verify nested dict responses are preserved (not converted to JSON)."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "location": {"lat": 10.5, "lon": 20.3},
    }

    # Act
    result = transform_submission_data(submission)

    # Assert: Dict preserved as-is
    responses = {r["question"]: r["response"] for r in result["responses"]}
    assert responses["location"] == {"lat": 10.5, "lon": 20.3}


def test_transform_empty_questions_yields_empty_responses():
    """Verify when only metadata fields exist, responses array is empty."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "_submission_time": "2025-11-01T12:00:00Z",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert
    assert result["_id"] == 1
    assert result["_uuid"] == "uuid-a"
    assert result["responses"] == []


def test_transform_preserves_all_metadata_fields():
    """Verify various _ prefixed fields stay in root object."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "_submission_time": "2025-11-01T12:00:00Z",
        "_submitted_by": "user1",
        "_attachments": [{"file": "img.jpg"}],
        "_notes": ["note1"],
        "_tags": ["tag1"],
        "_status": "approved",
        "question1": "answer1",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert: All metadata fields present (except excluded ones)
    assert result["_id"] == 1
    assert result["_uuid"] == "uuid-a"
    assert result["_submission_time"] == "2025-11-01T12:00:00Z"
    assert result["_submitted_by"] == "user1"
    assert result["_attachments"] == [{"file": "img.jpg"}]
    assert result["_notes"] == ["note1"]
    assert result["_tags"] == ["tag1"]
    assert result["_status"] == "approved"

    # Assert: Only question in responses
    assert len(result["responses"]) == 1
    assert result["responses"][0] == {"question": "question1", "response": "answer1"}


def test_responses_array_structure():
    """Verify each response has exact structure: {question, response}."""
    # Arrange
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "q1": "a1",
        "q2": "a2",
        "q3": "a3",
    }

    # Act
    result = transform_submission_data(submission)

    # Assert: Each response has exactly 2 keys
    for response in result["responses"]:
        assert set(response.keys()) == {"question", "response"}
        assert isinstance(response["question"], str)
        # response can be any type

    # Assert: Correct number of responses
    assert len(result["responses"]) == 3
