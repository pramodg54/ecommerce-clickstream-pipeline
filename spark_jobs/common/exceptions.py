"""
===============================================================================
File Name : exceptions.py
Project   : Ecommerce Clickstream Data Pipeline
Purpose   : Framework exception hierarchy.
Author    : Pramod Godse
===============================================================================
"""

from __future__ import annotations


class PipelineException(Exception):
    """
    Base exception for the ETL framework.
    """

    def __init__(
        self,
        message: str,
    ):

        super().__init__(message)

        self.message = message


# ==============================================================================
# Configuration
# ==============================================================================

class ConfigurationException(PipelineException):
    """
    Invalid or missing pipeline configuration.
    """
    pass


# ==============================================================================
# Read
# ==============================================================================

class DataReadException(PipelineException):
    """
    Raised when reading data fails.
    """
    pass


# ==============================================================================
# Write
# ==============================================================================

class DataWriteException(PipelineException):
    """
    Raised when writing data fails.
    """
    pass


# ==============================================================================
# Validation
# ==============================================================================

class ValidationException(PipelineException):
    """
    Raised when data validation fails.
    """
    pass


# ==============================================================================
# Metadata
# ==============================================================================

class MetadataException(PipelineException):
    """
    Raised when metadata creation fails.
    """
    pass


# ==============================================================================
# Transformation
# ==============================================================================

class TransformationException(PipelineException):
    """
    Raised during transformation failures.
    """
    pass


# ==============================================================================
# Pipeline
# ==============================================================================

class PipelineExecutionException(PipelineException):
    """
    Raised when a pipeline execution fails.
    """
    pass


# ==============================================================================
# Schema
# ==============================================================================

class SchemaValidationException(ValidationException):
    """
    Schema validation failure.
    """
    pass


# ==============================================================================
# Duplicate Keys
# ==============================================================================

class DuplicateKeyException(ValidationException):
    """
    Duplicate primary/business keys.
    """
    pass


# ==============================================================================
# Null Keys
# ==============================================================================

class NullKeyException(ValidationException):
    """
    Null values found in key columns.
    """
    pass


# ==============================================================================
# Referential Integrity
# ==============================================================================

class ReferentialIntegrityException(ValidationException):
    """
    Foreign key validation failure.
    """
    pass


# ==============================================================================
# Business Rules
# ==============================================================================

class BusinessRuleException(ValidationException):
    """
    Business rule validation failure.
    """
    pass