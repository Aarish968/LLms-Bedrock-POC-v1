import logging
from typing import TYPE_CHECKING, Iterable

from thoughtspot_tml._scriptability import (
    ActionContextE,
    ActionObjectAssociationEdocProto,
)

if TYPE_CHECKING:
    from thoughtspot_tml import Liveboard


logger = logging.getLogger(__name__)

CUSTOM_ACTIONS = ["Extract", "Tag", "UnTag"]


def update_table_for_visualizations(
    table_detail: dict, liveboard: "Liveboard"
) -> "Liveboard":
    """
    Updates Visualizations tables for the Liveboard
    """

    visualizations = liveboard.liveboard.visualizations
    if visualizations:
        for viz in visualizations:
            if viz and viz.answer:
                viz.answer.tables = [table_detail]

    liveboard.liveboard.visualizations = visualizations

    return liveboard


def update_worksheet_references_in_liveboard(
    liveboard: "Liveboard",
    new_worksheet_name: str,
) -> "Liveboard":
    """
    Updates worksheet references in parameter_overrides and ordered_chips.

    Replaces any worksheet name before "::" with the new worksheet name.
    Example: "OldWorksheet::Parameter Name" -> "ws_canvas-12345::Parameter Name"

    Args:
        liveboard: Liveboard TML object
        new_worksheet_name: New worksheet name (e.g., "ws_canvas-37684")

    Returns:
        Liveboard: Updated liveboard object

    Note:
        parameter_overrides: list of dicts [{'key': '...', 'value': {'name': '...'}}]
        ordered_chips: list of OrderedChipEDocProto objects with .name attribute
    """
    replacements_made = 0

    # Update parameter_overrides (list of dicts with 'value': {'name': '...'})
    if (
        hasattr(liveboard.liveboard, "parameter_overrides")
        and liveboard.liveboard.parameter_overrides
    ):
        for param in liveboard.liveboard.parameter_overrides:
            if isinstance(param, dict) and "value" in param:
                value_dict = param["value"]
                if isinstance(value_dict, dict) and "name" in value_dict:
                    current_name = value_dict["name"]
                    if current_name and "::" in current_name:
                        parts = current_name.split("::", 1)
                        if len(parts) == 2:
                            new_name = f"{new_worksheet_name}::{parts[1]}"
                            value_dict["name"] = new_name
                            logger.info(
                                f"Updated parameter_override '{current_name}' -> '{new_name}'"
                            )
                            replacements_made += 1

    # Update ordered_chips (list of OrderedChipEDocProto objects)
    if (
        hasattr(liveboard.liveboard, "ordered_chips")
        and liveboard.liveboard.ordered_chips
    ):
        for chip in liveboard.liveboard.ordered_chips:
            if hasattr(chip, "name") and chip.name and "::" in chip.name:
                parts = chip.name.split("::", 1)
                if len(parts) == 2:
                    old_name = chip.name
                    chip.name = f"{new_worksheet_name}::{parts[1]}"
                    logger.info(f"Updated ordered_chip '{old_name}' -> '{chip.name}'")
                    replacements_made += 1

    if replacements_made > 0:
        logger.info(f"Total worksheet references updated: {replacements_made}")
    else:
        logger.debug(
            "No worksheet references found to update in parameter_overrides or ordered_chips"
        )

    return liveboard


def add_custom_actions_in_tml(liveboard: "Liveboard") -> ("Liveboard", list[str]):
    """
    Adds custom actions to Liveboard object.
    :param liveboard TML Liveboard object
    :return tuple(TML Liveboard object, list of updated visualization GUIDs)
    """

    def custom_action_proto(name: str) -> ActionObjectAssociationEdocProto:
        # noinspection PyTypeChecker
        return ActionObjectAssociationEdocProto(
            action_name=name, context=ActionContextE.CONTEXT_MENU.name, enabled=True
        )

    updated_guids = []
    for viz in liveboard.liveboard.visualizations:
        if viz.answer and (viz.answer.chart.type == "KPI" or viz.answer.formulas):
            continue

        if not viz.action_object_associations:
            guid = viz.viz_guid
            logger.info(f"Adding custom actions for {guid=}")
            viz.action_object_associations = list(
                map(custom_action_proto, CUSTOM_ACTIONS)
            )
            updated_guids.append(guid)

    return liveboard, updated_guids
