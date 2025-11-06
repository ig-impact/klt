from datetime import timedelta


def test_no_assets_yields_no_submissions(
    kobo_submission_factory,
    rest_client_stub,
    run_pipeline_once,
    query,
):
    """Asset resource returns empty (0 assets), verify submission table is empty."""
    # Arrange: No assets
    rest_client_stub.set_for_path("project-views", [])

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_no_assets")

    # Assert: Table should not be created (no data flows through)
    result = query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'submissions_no_assets'"
    )
    assert len(result) == 0, "Table should not be created when no assets exist"


def test_cursor_below_initial_value_skipped(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    query,
):
    """Submission with _submission_time < submission_time_start, verify filtered out."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Submission BEFORE submission_time_start (2025-01-01)
    # base_time is 2025-11-01, so -timedelta(days=310) = ~2024-12-27
    old_submission = submission_builder(
        id=1, submission_time_offset=-timedelta(days=310)
    )
    rest_client_stub.set_for_path("assets/asset-1/data", [old_submission])

    # Act
    resource = kobo_submission_factory(submission_time_start="2025-01-01T00:00:00Z")
    run_pipeline_once(resource, table_name="submissions_old")

    # Assert: Table not created (submission filtered by query)
    result = query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'submissions_old'"
    )
    assert len(result) == 0, "Old submissions should be filtered by MongoDB query"


def test_cursor_equality_is_loaded(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """Submission with _submission_time == submission_time_start, verify loaded (using $gte)."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Submission exactly at submission_time_start
    # We need to create a submission with _submission_time = "2025-01-01T00:00:00Z"
    exact_submission = {
        "_id": 1,
        "_uuid": "uuid-exact",
        "_submission_time": "2025-01-01T00:00:00Z",
        "_submitted_by": "test_user",
        "question1": "answer1",
    }
    rest_client_stub.set_for_path("assets/asset-1/data", [exact_submission])

    # Act
    resource = kobo_submission_factory(submission_time_start="2025-01-01T00:00:00Z")
    run_pipeline_once(resource, table_name="submissions_exact")

    # Assert: Submission loaded (>= behavior)
    assert_table_count("submissions_exact", 1)


def test_second_run_deduplicates_overlapping_submissions(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_twice,
    assert_table_count,
):
    """First run: A,B,C. Second run: C,D (C is duplicate). Verify only 4 unique rows."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: First run submissions
    first_pages = [
        [
            submission_builder(
                id=1, uuid_val="uuid-a", submission_time_offset=timedelta(days=30)
            ),
            submission_builder(
                id=2, uuid_val="uuid-b", submission_time_offset=timedelta(days=31)
            ),
            submission_builder(
                id=3, uuid_val="uuid-c", submission_time_offset=timedelta(days=32)
            ),
        ]
    ]

    # Arrange: Second run - overlapping submission (id=3)
    second_pages = [
        [
            submission_builder(
                id=3, uuid_val="uuid-c", submission_time_offset=timedelta(days=32)
            ),  # Duplicate
            submission_builder(
                id=4, uuid_val="uuid-d", submission_time_offset=timedelta(days=33)
            ),
        ]
    ]

    # Act
    resource = kobo_submission_factory()
    run_twice(
        resource,
        table="submissions_dedup_overlap",
        first_pages=first_pages,
        second_pages=second_pages,
    )

    # Assert: 4 unique submissions (A, B, C, D)
    assert_table_count("submissions_dedup_overlap", 4)


def test_mixed_submission_times_in_single_page(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """Single page with submissions spanning multiple dates, verify all loaded correctly."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Submissions with various dates in single page
    mixed_page = [
        submission_builder(id=1, submission_time_offset=timedelta(days=10)),
        submission_builder(id=2, submission_time_offset=timedelta(days=50)),
        submission_builder(id=3, submission_time_offset=timedelta(days=5)),
        submission_builder(id=4, submission_time_offset=timedelta(days=100)),
    ]
    rest_client_stub.set_for_path("assets/asset-1/data", mixed_page)

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_mixed_times")

    # Assert: All loaded
    assert_table_count("submissions_mixed_times", 4)


def test_submission_with_empty_strings(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    run_pipeline_once,
    assert_table_count,
    query,
):
    """Questions with empty string responses, verify preserved (not filtered)."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Submission with empty string responses
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "_submission_time": "2025-11-01T12:00:00Z",
        "_submitted_by": "test_user",
        "filled_field": "data",
        "empty_field": "",
    }
    rest_client_stub.set_for_path("assets/asset-1/data", [submission])

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_empty_strings")

    # Assert: 1 submission with 2 responses (including empty string)
    assert_table_count("submissions_empty_strings", 1)

    # Verify responses array contains both (DLT stores nested arrays in child tables)
    result = query('SELECT COUNT(*) FROM "submissions_empty_strings__responses"')
    assert result[0][0] == 2, "Should have 2 response rows in child table"


def test_large_response_array(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    run_pipeline_once,
    assert_table_count,
    query,
):
    """Submission with many questions (50+), verify all converted to EAV correctly."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Submission with 50 questions
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "_submission_time": "2025-11-01T12:00:00Z",
    }
    # Add 50 questions
    for i in range(1, 51):
        submission[f"question_{i}"] = f"answer_{i}"

    rest_client_stub.set_for_path("assets/asset-1/data", [submission])

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_large")

    # Assert: 1 submission loaded
    assert_table_count("submissions_large", 1)

    # Verify responses array has 50 items (DLT stores nested arrays in child tables)
    result = query('SELECT COUNT(*) FROM "submissions_large__responses"')
    assert result[0][0] == 50, "Should have 50 response rows in child table"


def test_special_characters_in_question_names(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    run_pipeline_once,
    assert_table_count,
    query,
):
    """Questions with special characters in names, verify handled correctly."""
    # Arrange: Create asset
    asset = asset_builder(uid="asset-1")
    rest_client_stub.set_for_path("project-views", [asset])

    # Arrange: Submission with special characters in question names
    submission = {
        "_id": 1,
        "_uuid": "uuid-a",
        "_submission_time": "2025-11-01T12:00:00Z",
        "question/with/slashes": "answer1",
        "question-with-dashes": "answer2",
        "question_with_underscores": "answer3",
        "question.with.dots": "answer4",
    }
    rest_client_stub.set_for_path("assets/asset-1/data", [submission])

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_special_chars")

    # Assert: 1 submission with 4 responses
    assert_table_count("submissions_special_chars", 1)

    # Verify all questions preserved (DLT stores nested arrays in child tables)
    result = query('SELECT COUNT(*) FROM "submissions_special_chars__responses"')
    assert result[0][0] == 4, "Should have 4 response rows in child table"

    # Verify question names are preserved correctly
    result = query('SELECT question FROM "submissions_special_chars__responses"')
    question_names = {row[0] for row in result}
    assert "question/with/slashes" in question_names
    assert "question-with-dashes" in question_names
    assert "question_with_underscores" in question_names
    assert "question.with.dots" in question_names


def test_parallel_processing_with_multiple_assets(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    submission_builder,
    run_pipeline_once,
    assert_table_count,
):
    """parallelized=True (default), multiple assets yielding submissions concurrently."""
    # Arrange: Create 3 assets
    asset1 = asset_builder(uid="asset-1")
    asset2 = asset_builder(uid="asset-2")
    asset3 = asset_builder(uid="asset-3")
    rest_client_stub.set_for_path("project-views", [asset1, asset2, asset3])

    # Arrange: Submissions for each asset
    rest_client_stub.set_for_path(
        "assets/asset-1/data",
        [submission_builder(id=1, submission_time_offset=timedelta(days=30))],
    )
    rest_client_stub.set_for_path(
        "assets/asset-2/data",
        [submission_builder(id=2, submission_time_offset=timedelta(days=31))],
    )
    rest_client_stub.set_for_path(
        "assets/asset-3/data",
        [submission_builder(id=3, submission_time_offset=timedelta(days=32))],
    )

    # Act: Default parallelized=True
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_parallel")

    # Assert: All 3 submissions loaded without data corruption
    assert_table_count("submissions_parallel", 3)


def test_asset_with_uid_but_no_submissions(
    kobo_submission_factory,
    rest_client_stub,
    asset_builder,
    run_pipeline_once,
    assert_table_count,
):
    """Valid asset UID but API returns empty results, verify no error."""
    # Arrange: Create 2 assets
    asset1 = asset_builder(uid="asset-1")
    asset2 = asset_builder(uid="asset-2")
    rest_client_stub.set_for_path("project-views", [asset1, asset2])

    # Arrange: asset-1 has no submissions, asset-2 has 1
    rest_client_stub.set_for_path("assets/asset-1/data", [])
    rest_client_stub.set_for_path(
        "assets/asset-2/data",
        [
            {
                "_id": 1,
                "_uuid": "uuid-a",
                "_submission_time": "2025-11-01T12:00:00Z",
                "_submitted_by": "test_user",
                "question1": "answer1",
            }
        ],
    )

    # Act
    resource = kobo_submission_factory()
    run_pipeline_once(resource, table_name="submissions_partial")

    # Assert: Only 1 submission from asset-2
    assert_table_count("submissions_partial", 1)
