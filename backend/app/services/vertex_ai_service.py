"""Vertex AI service for AI-powered test generation."""
import json
from typing import List, Dict, Any
from google.cloud import aiplatform
from vertexai.generative_models import GenerativeModel
import vertexai

from app.config import settings

import logging
logger = logging.getLogger(__name__)


class VertexAIService:
    """Service for Vertex AI operations."""
    
    def __init__(self):
        """Initialize Vertex AI service."""
        self.model = None

    def _ensure_model(self):
        """Ensure model is initialized."""
        if not self.model:
            vertexai.init(
                project=settings.google_cloud_project,
                location=settings.vertex_ai_location
            )
            self.model = GenerativeModel(settings.vertex_ai_model)
    
    async def generate_test_suggestions(
        self,
        mapping_id: str,
        source_info: str,
        target_table: str,
        bq_schema: Dict,
        gcs_sample: List[Dict],
        bq_sample: List[Dict],
        existing_tests: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Generate AI test suggestions for a data mapping.
        
        Args:
            mapping_id: Mapping identifier
            source_info: Source file information
            target_table: Target table name
            bq_schema: BigQuery schema
            gcs_sample: Sample data from GCS
            bq_sample: Sample data from BigQuery
            existing_tests: List of existing test names
            
        Returns:
            List of test suggestions
        """
        prompt = f"""
You are a data quality expert analyzing a data pipeline.

**Context:**
- Mapping: {mapping_id}
- Source: {source_info}
- Target: {target_table}
- Schema: {json.dumps(bq_schema, indent=2)}
- GCS Sample: {json.dumps(gcs_sample[:5], indent=2, default=str)}
- BigQuery Sample: {json.dumps(bq_sample[:5], indent=2, default=str)}

**Predefined Tests Already Running:**
{chr(10).join(f'- {test}' for test in existing_tests)}

**Your Task:**
Suggest 3-5 ADDITIONAL test cases that would be valuable for this specific dataset.
Focus on:
1. Business logic specific to this data
2. Data patterns you observe in the samples
3. Potential data quality issues not covered by standard tests

For each suggestion, provide:
- test_name: Clear, descriptive name
- test_category: One of (completeness/integrity/quality/statistical/business)
- severity: HIGH/MEDIUM/LOW
- sql_query: Complete SQL query that returns rows that FAIL the test
- reasoning: Why this test is important for THIS specific dataset (2-3 sentences)

CRITICAL: Use the full table name `{target_table}` in all SQL queries.

Return ONLY a JSON array of 3-5 suggestions. No markdown formatting.
"""
        
        try:
            self._ensure_model()
            response = self.model.generate_content(prompt)
            text = response.text
            
            # Clean up markdown formatting if present
            text = text.replace('```json\n', '').replace('```\n', '').replace('```', '').strip()
            
            # Parse JSON
            suggestions = json.loads(text)
            
            return suggestions if isinstance(suggestions, list) else []
            
        except Exception as e:
            print(f"Failed to generate AI suggestions: {str(e)}")
            return []

    async def validate_schema(
        self,
        erd_description: str,
        actual_schemas: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Validate actual BigQuery schemas against the ERD description.
        
        Args:
            erd_description: Description of the expected ERD
            actual_schemas: Dictionary mapping table names to their BigQuery schemas
            
        Returns:
            List of schema issues or suggestions
        """
        prompt = f"""
You are a data architect validating a database implementation against a design.

**Design Description (ERD):**
{erd_description}

**Actual Implemented Schemas (BigQuery):**
{json.dumps(actual_schemas, indent=2)}

**Your Task:**
Compare the actual implementation against the design. Identify:
1. Missing tables
2. Missing columns
3. Incorrect data types
4. Potential foreign key relationships implied in the design but not obvious in schema

For each check, provide:
- test_name: Descriptive name
- test_category: "schema_validation"
- severity: HIGH/MEDIUM/LOW
- status: PASS/FAIL/WARNING
- reasoning: Explanation of the finding
- sql_query: If applicable, a query to check for this issue (or empty string)

Return ONLY a JSON array of findings. No markdown.
"""
        try:
            self._ensure_model()
            response = self.model.generate_content(prompt)
            text = response.text
            text = text.replace('```json\\n', '').replace('```\\n', '').replace('```', '').strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"Failed to validate schema: {str(e)}")
            return []



# Singleton instance
vertex_ai_service = VertexAIService()
