import datetime
from typing import Optional, Tuple

from datacode.logger import logger
from datacode.models.links import LinkedItem


class LinkedLastModifiedItem(LinkedItem):
    last_modified: Optional[datetime.datetime]

    @property
    def pipeline_last_modified(self) -> Optional[datetime.datetime]:
        logger.debug(f'Determining pipeline last modified for {self}')
        lm = _nested_most_recent_last_modified(None, self)
        logger.debug(f'Finished determining pipeline last modified for {self}')
        return lm

    @property
    def pipeline_obj_last_modified(self) -> Tuple['LinkedLastModifiedItem', Optional[datetime.datetime]]:
        logger.debug(f'Determining pipeline object last modified for {self}')
        lm = _nested_most_recent_obj_last_modified(None, self, self)
        logger.debug(f'Finished determining pipeline object last modified for {self}')
        return lm


def _nested_most_recent_last_modified(
    lm: Optional[datetime.datetime], item: LinkedLastModifiedItem
) -> Optional[datetime.datetime]:
    sub_item: LinkedLastModifiedItem
    for sub_item in item.back_links:
        lm = most_recent_last_modified(lm, sub_item.last_modified)
        lm = _nested_most_recent_last_modified(lm, sub_item)
    return lm


def _nested_most_recent_obj_last_modified(
    lm: Optional[datetime.datetime],
    lm_item: LinkedLastModifiedItem,
    item: LinkedLastModifiedItem,
) -> Tuple[LinkedLastModifiedItem, Optional[datetime.datetime]]:
    out_item = lm_item
    sub_item: LinkedLastModifiedItem
    for sub_item in item.back_links:
        out_item, lm = most_recent_obj_last_modified(lm, out_item, sub_item.last_modified, sub_item)
        out_item, lm = _nested_most_recent_obj_last_modified(lm, out_item, sub_item)
    return out_item, lm


def most_recent_last_modified(
    lm1: Optional[datetime.datetime], lm2: Optional[datetime.datetime]
) -> Optional[datetime.datetime]:
    if lm1 is None:
        return lm2
    if lm2 is None:
        return lm1

    return max(lm1, lm2)


def most_recent_obj_last_modified(
    lm1: Optional[datetime.datetime],
    item1: LinkedLastModifiedItem,
    lm2: Optional[datetime.datetime],
    item2: LinkedLastModifiedItem,
) -> Tuple[LinkedLastModifiedItem, Optional[datetime.datetime]]:
    if lm1 is None:
        return item2, lm2
    if lm2 is None:
        return item1, lm1

    max_lm = max(lm1, lm2)
    if max_lm == lm1:
        return item1, lm1
    else:
        return item2, lm2