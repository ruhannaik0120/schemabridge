from uuid import UUID, uuid4

from schemabridge.services.jobs.queue import MigrationJobPublisher


class RecordingPublisher:
    def __init__(self) -> None:
        self.published_job_ids: list[UUID] = []

    def publish(self, job_id: UUID) -> None:
        self.published_job_ids.append(job_id)


def test_recording_publisher_follows_the_queue_rule() -> None:
    publisher = RecordingPublisher()
    job_id = uuid4()

    assert isinstance(publisher, MigrationJobPublisher)

    publisher.publish(job_id)

    assert publisher.published_job_ids == [job_id]