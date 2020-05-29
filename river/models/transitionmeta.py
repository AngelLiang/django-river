from __future__ import unicode_literals

from django.db import models
from django.db.models import PROTECT
from django.utils.translation import ugettext_lazy as _

from river.models import State, Workflow
from river.models.base_model import BaseModel


class TransitionMeta(BaseModel):
    """流转元数据"""
    class Meta:
        app_label = 'river'
        verbose_name = _("Transition Meta")
        verbose_name_plural = _("Transition Meta")
        unique_together = [('workflow', 'source_state', 'destination_state')]

    # 工作流
    workflow = models.ForeignKey(Workflow, verbose_name=_("Workflow"), related_name='transition_metas', on_delete=PROTECT)
    # 源状态
    source_state = models.ForeignKey(State, verbose_name=_("Source State"), related_name='transition_meta_as_source', on_delete=PROTECT)
    # 目的状态
    destination_state = models.ForeignKey(State, verbose_name=_("Destination State"), related_name='transition_meta_as_destination', on_delete=PROTECT)

    def __str__(self):
        return 'Field Name:%s, %s -> %s' % (
            self.workflow,
            self.source_state,
            self.destination_state
        )
