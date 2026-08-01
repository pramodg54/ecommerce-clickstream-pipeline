"""
===============================================================================
File Name : pipeline_metrics.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Pipeline execution metrics.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class PipelineMetrics:

    pipeline_name: str

    start_time: datetime = field(default_factory=datetime.utcnow)

    end_time: datetime | None = None

    duration_seconds: float = 0.0

    rows_read: int = 0

    rows_written: int = 0

    rows_rejected: int = 0

    status: str = "RUNNING"

    error_message: str | None = None

    # ==============================================================
    # Lifecycle
    # ==============================================================

    def finish_success(self):

        self.end_time = datetime.utcnow()

        self.duration_seconds = (

            self.end_time - self.start_time

        ).total_seconds()

        self.status = "SUCCESS"

    def finish_failure(
        self,
        message: str,
    ):

        self.end_time = datetime.utcnow()

        self.duration_seconds = (

            self.end_time - self.start_time

        ).total_seconds()

        self.status = "FAILED"

        self.error_message = message

    # ==============================================================
    # Logging
    # ==============================================================

    def log(self, logger):

        logger.info("=" * 80)

        logger.info(
            "Pipeline : %s",
            self.pipeline_name,
        )

        logger.info(
            "Status : %s",
            self.status,
        )

        logger.info(f"Rows Read : {self.rows_read:,}")

        logger.info(f"Rows Written : {self.rows_written:,}")
        
        logger.info(f"Rows Rejected : {self.rows_rejected:,}")

        logger.info(
            "Duration : %.2f sec",
            self.duration_seconds,
        )

        if self.error_message:

            logger.error(
                self.error_message
            )

        logger.info("=" * 80)

    # ==============================================================
    # Dictionary
    # ==============================================================

    def as_dict(self):

        return {

            "pipeline_name": self.pipeline_name,

            "status": self.status,

            "rows_read": self.rows_read,

            "rows_written": self.rows_written,

            "rows_rejected": self.rows_rejected,

            "duration_seconds": self.duration_seconds,

            "start_time": self.start_time,

            "end_time": self.end_time,

            "error_message": self.error_message,
        }