from django.contrib import auth
from django.db.models import Min, CharField, Q, F
from django.db.models.functions import Cast
from django_cte import With

from river.driver.river_driver import RiverDriver
from river.models import TransitionApproval, PENDING


class OrmDriver(RiverDriver):

    def get_available_approvals(self, as_user):
        """获取可用的流转"""

        those_with_max_priority = With(
            TransitionApproval.objects.filter(
                # 状态为 PENDING 的 TransitionApproval
                workflow=self.workflow, status=PENDING
            ).values(
                'workflow', 'object_id', 'transition'
            ).annotate(min_priority=Min('priority'))
        )

        workflow_objects = With(
            # wokflow_object_class 关联的对象类
            self.wokflow_object_class.objects.all(),
            name="workflow_object"
        )

        # 最大优先级的批准
        approvals_with_max_priority = those_with_max_priority.join(
            self._authorized_approvals(as_user),
            workflow_id=those_with_max_priority.col.workflow_id,
            object_id=those_with_max_priority.col.object_id,
            transition_id=those_with_max_priority.col.transition_id,
        ).with_cte(
            those_with_max_priority
        ).annotate(
            # 对象ID
            object_id_as_str=Cast('object_id', CharField(max_length=200)),
            # 最小的priority
            min_priority=those_with_max_priority.col.min_priority
        ).filter(min_priority=F("priority"))

        return workflow_objects.join(
            approvals_with_max_priority, object_id_as_str=Cast(workflow_objects.col.pk, CharField(max_length=200))
        ).with_cte(
            workflow_objects
        # 流转源状态为 field_name 字段
        ).filter(transition__source_state=getattr(workflow_objects.col, self.field_name + "_id"))

    def _authorized_approvals(self, as_user):
        # 获取用户所有的权限组
        group_q = Q()
        for g in as_user.groups.all():
            group_q = group_q | Q(groups__in=[g])

        # 获取用户的所有权限
        permissions = []
        for backend in auth.get_backends():
            permissions.extend(backend.get_all_permissions(as_user))

        permission_q = Q()
        for p in permissions:
            label, codename = p.split('.')
            permission_q = permission_q | Q(permissions__content_type__app_label=label,
                                            permissions__codename=codename)

        return TransitionApproval.objects.filter(
            Q(workflow=self.workflow, status=PENDING) &
            (
                    # 流转者为空 或 流转者是当前用户
                    (Q(transactioner__isnull=True) | Q(transactioner=as_user)) &
                    # 权限为空 或 和用户有同一权限
                    (Q(permissions__isnull=True) | permission_q) &
                    # 权限组为空 或 和用户处于同一权限组
                    (Q(groups__isnull=True) | group_q)
            )
        )
