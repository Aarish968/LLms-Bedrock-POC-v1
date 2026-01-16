import copy
import json
import logging
from json.decoder import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, Optional

from thoughtspot_tml.tml import Worksheet
from yaml import safe_load as yaml_safe_load

from dc_canvas_service.services.s3.exceptions import S3DownloadFileException

if TYPE_CHECKING:
    from dc_canvas_service.common import Settings
    from dc_canvas_service.services.s3 import S3Service

logger = logging.getLogger(__name__)


class FormulaTemplateService:
    """
    FormulaTemplateService is the service class for handling formula templates in ThoughtSpot worksheets.

    This service is responsible for downloading worksheet templates from S3, extracting formulas and other
    elements from templates, and applying them to target worksheets with proper reference updates.
    """

    def __init__(self, s3_service: "S3Service", settings: "Settings"):
        """
        Initialize the FormulaTemplateService.

        Args:
            s3_service: S3 service for file operations
            settings: Application settings containing formula template configuration
        """
        self.s3 = s3_service
        self.settings = settings

    def download_worksheet_template(self, env: Optional[str] = None) -> Worksheet:
        """
        Downloads worksheet template from S3 bucket.

        Retrieves the formula template worksheet from the configured S3 bucket
        using the environment-specific path and filename from settings.

        Args:
            env: Optional environment to use, defaults to current environment

        Returns:
            Worksheet: Worksheet template object loaded from S3
        """
        environment = env if env else self.settings.env
        s3_location = (
            f"s3://{self.settings.ts_data_types_bucket_name}/"
            f"{environment}/{self.settings.ts_formulas_template_filename}"
        )

        return self._create_worksheet_from_s3(s3_location)

    def _create_worksheet_from_s3(self, s3_location: str) -> Worksheet:
        """
        Creates a Worksheet object from a TML file stored in S3.

        Downloads the file from S3, parses it as JSON or YAML, and creates
        a Worksheet object from the parsed content.

        Args:
            s3_location: Location of the TML Object File in S3

        Returns:
            Worksheet: Worksheet Object created using the file from S3
        """
        bucket, key = tuple(self.s3.parse_uri(s3_location))

        bytes_val = self.s3.download_file(bucket, key)
        tml_str = bytes_val.decode("utf-8")
        try:
            tml_file = json.loads(tml_str)
        except JSONDecodeError:
            tml_file = yaml_safe_load(tml_str)

        tml_file["guid"] = ""
        worksheet = Worksheet(**tml_file)
        worksheet.guid = None

        return worksheet

    def get_formula_columns(self) -> Dict:
        """
        Downloads formula template from S3 and returns the formula elements.

        This method serves as the main entry point for retrieving formula elements
        from a template. It downloads the template and extracts its components.
        If the template file is not available in S3, it gracefully returns empty
        results instead of failing.

        Returns:
            Dict: Dictionary containing parameters, formulas, and worksheet_columns
                 extracted from the template along with the template's table_path_id.
                 Returns empty dictionary with default structure if template is not available.
        """
        try:
            # Download template from S3
            template_worksheet = self.download_worksheet_template()

            # Extract formulas, parameters and formula columns
            template_elements = self.extract_formulas_from_template(template_worksheet)
        except S3DownloadFileException:
            logger.info(
                "Formula template file not found in S3, skipping formula processing"
            )
            # Return empty template elements structure
            return {
                "parameters": [],
                "formulas": [],
                "worksheet_columns": [],
                "table_path_id": None,
            }
        else:
            return template_elements

    def extract_formulas_from_template(self, template_worksheet: Worksheet) -> Dict:
        """
        Extracts parameters, formulas and worksheet columns from a template worksheet.

        Analyzes the provided template worksheet to extract formula-related elements,
        including parameters, formulas, and worksheet columns that have a formula_id.
        Also identifies the template's table path ID for reference replacement.

        Args:
            template_worksheet: The worksheet template to extract from

        Returns:
            Dict: Dictionary containing 'parameters', 'formulas', 'worksheet_columns',
                 and 'table_path_id' extracted from the template
        """
        result = {
            "parameters": [],
            "formulas": [],
            "worksheet_columns": [],
            "table_path_id": None,  # Store the template table path ID for reference
        }

        # Extract table_paths for reference replacement
        if (
            hasattr(template_worksheet.worksheet, "table_paths")
            and template_worksheet.worksheet.table_paths
        ):
            # Get the first table path ID that matches the pattern
            for path in template_worksheet.worksheet.table_paths:
                # Handle both dictionary and object cases
                if isinstance(path, dict) and "id" in path:
                    path_id = path["id"]
                else:
                    path_id = getattr(path, "id", "")

                if path_id and "CANVAS_" in path_id:
                    result["table_path_id"] = path_id
                    break

        # Extract parameters if they exist
        if hasattr(template_worksheet.worksheet, "parameters"):
            result["parameters"] = template_worksheet.worksheet.parameters

        # Extract formulas if they exist
        if hasattr(template_worksheet.worksheet, "formulas"):
            result["formulas"] = template_worksheet.worksheet.formulas

        # Extract worksheet columns with formula_id instead of column_id
        if hasattr(template_worksheet.worksheet, "worksheet_columns"):
            result["worksheet_columns"] = [
                column
                for column in template_worksheet.worksheet.worksheet_columns
                if hasattr(column, "formula_id") and column.formula_id
            ]

        return result

    @staticmethod
    def _get_worksheet_table_path_id(worksheet: Worksheet) -> Optional[str]:
        """
        Get the table_path.id from a worksheet.

        Extracts the table path ID from the worksheet's table_paths, looking for
        paths that match the CANVAS_ pattern.

        Args:
            worksheet: The worksheet to extract the table_path.id from

        Returns:
            Optional[str]: The table_path.id or None if not found
        """
        if (
            hasattr(worksheet.worksheet, "table_paths")
            and worksheet.worksheet.table_paths
        ):
            # Try to find the first table path with the CANVAS_ pattern
            for path in worksheet.worksheet.table_paths:
                # Handle both dictionary and object cases
                if isinstance(path, dict) and "id" in path:
                    path_id = path["id"]
                else:
                    path_id = getattr(path, "id", "")

                if path_id and "CANVAS_" in path_id and "_THOUGHT_SPOT_V" in path_id:
                    return path_id
        return None

    @staticmethod
    def update_formula_expr_references(
        formula: Any, old_path_id: str, new_path_id: str
    ) -> bool:
        """
        Update references to table_path.id in formula.expr.

        Replaces all occurrences of the old table path ID with the new one
        in the formula expression.

        Args:
            formula: The formula object to update
            old_path_id: The old table_path.id to replace
            new_path_id: The new table_path.id to use

        Returns:
            bool: True if updates were made, False otherwise
        """
        if hasattr(formula, "expr"):
            old_expr = formula.expr
            if old_expr and old_path_id in old_expr:
                # Replace all occurrences of old_path_id with new_path_id
                formula.expr = old_expr.replace(old_path_id, new_path_id)
                return True
        return False

    @staticmethod
    def validate_no_template_references(
        worksheet: Worksheet, template_table_path_id: str
    ) -> bool:
        """
        Validate that no references to the template table path ID remain in the worksheet formulas.

        Checks all formulas in the worksheet to ensure none of them still contain
        references to the original template table path ID.

        Args:
            worksheet: The worksheet to validate
            template_table_path_id: The table path ID from the template that should not be present

        Returns:
            bool: True if validation passes (no references found), False otherwise
        """
        if (
            not hasattr(worksheet.worksheet, "formulas")
            or not worksheet.worksheet.formulas
        ):
            return True

        for formula in worksheet.worksheet.formulas:
            if (
                hasattr(formula, "expr")
                and formula.expr
                and template_table_path_id in formula.expr
            ):
                # Found a formula that still contains the template table path ID
                return False

        return True

    def append_formula_elements_to_worksheet(
        self, target_worksheet: Worksheet, template_elements: Dict
    ) -> Worksheet:
        """
        Merges formula elements into a target worksheet with de-duplication and updates references.

        This method intelligently merges parameters, formulas, and formula worksheet columns from
        the template into a target worksheet. For parameters and formulas, it implements de-duplication
        logic where:
        - Template elements ALWAYS take precedence (template overwrites existing items with same name)
        - Target worksheet elements are preserved only if their names don't exist in the template

        The method also updates all references to table path IDs from the template to match the
        current worksheet's table paths, ensuring proper formula functionality.

        The method performs thorough validation to ensure no template references remain
        in the worksheet after updates, and verifies that formula updates were actually
        made when table path IDs differ.

        Args:
            target_worksheet: The worksheet to merge elements into
            template_elements: Dictionary containing parameters, formulas and worksheet_columns to merge

        Returns:
            Worksheet: The updated worksheet with formula elements merged and references updated

        Raises:
            ValueError: If formula references could not be properly updated or if template
                       references remain after updates
        """
        # Get template table path ID and current worksheet table path ID for replacement
        template_table_path_id = template_elements.get("table_path_id")
        current_table_path_id = self._get_worksheet_table_path_id(target_worksheet)

        # Merge parameters with de-duplication: template takes precedence
        if template_elements.get("parameters"):
            # Get existing parameters from target worksheet
            existing_parameters = (
                target_worksheet.worksheet.parameters
                if hasattr(target_worksheet.worksheet, "parameters")
                and target_worksheet.worksheet.parameters
                else []
            )

            # Create a set of parameter names from template
            template_param_names = {
                param.name for param in template_elements["parameters"]
            }

            # Keep only target parameters that don't exist in template
            unique_target_parameters = [
                param
                for param in existing_parameters
                if param.name not in template_param_names
            ]

            # Combine: all template parameters + unique target parameters
            target_worksheet.worksheet.parameters = (
                list(template_elements["parameters"]) + unique_target_parameters
            )

        # Append formulas if they exist
        if template_elements.get("formulas"):
            # Make a deep copy of the formulas to avoid modifying the template
            formulas = copy.deepcopy(template_elements["formulas"])

            # Always update formula references if both table path IDs are available
            if template_table_path_id and current_table_path_id:
                # Only verify updates when IDs are different
                need_to_verify = template_table_path_id != current_table_path_id
                found_reference = False
                made_update = False

                # Process all formulas
                for formula in formulas:
                    # Check if formula contains a reference that needs updating
                    if (
                        hasattr(formula, "expr")
                        and formula.expr
                        and template_table_path_id in formula.expr
                    ):
                        found_reference = True

                    # Try to update references
                    if self.update_formula_expr_references(
                        formula, template_table_path_id, current_table_path_id
                    ):
                        made_update = True

                # If IDs are different, we found formulas with references, but no updates were made - that's an error
                if need_to_verify and found_reference and not made_update:
                    error_message = f"Formula references to '{template_table_path_id}' were not updated properly"
                    raise ValueError(error_message)

                # Validate that all references were replaced
                error_message = f"Failed to replace all template table path IDs '{template_table_path_id}' in formulas"
                if not self.validate_no_template_references(
                    target_worksheet, template_table_path_id
                ):
                    raise ValueError(error_message)

            # Merge formulas with de-duplication: template takes precedence
            # Get existing formulas from target worksheet
            existing_formulas = (
                target_worksheet.worksheet.formulas
                if hasattr(target_worksheet.worksheet, "formulas")
                and target_worksheet.worksheet.formulas
                else []
            )

            # Create a set of formula names from template
            template_formula_names = {formula.name for formula in formulas}

            # Keep only target formulas that don't exist in template
            unique_target_formulas = [
                formula
                for formula in existing_formulas
                if formula.name not in template_formula_names
            ]

            # Combine: all template formulas + unique target formulas
            target_worksheet.worksheet.formulas = formulas + unique_target_formulas

            # Final validation after adding formulas to ensure no template references remain
            error_message = (
                "Failed to replace all template table path IDs in worksheet formulas"
            )
            if (
                template_table_path_id
                and current_table_path_id
                and not self.validate_no_template_references(
                    target_worksheet, template_table_path_id
                )
            ):
                raise ValueError(error_message)

        # Append formula worksheet columns if they exist
        if template_elements.get("worksheet_columns"):
            if (
                not hasattr(target_worksheet.worksheet, "worksheet_columns")
                or target_worksheet.worksheet.worksheet_columns is None
            ):
                target_worksheet.worksheet.worksheet_columns = []
            target_worksheet.worksheet.worksheet_columns.extend(
                template_elements["worksheet_columns"]
            )

        return target_worksheet
