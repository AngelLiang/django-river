from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from hamcrest import is_not, assert_that, has_key, has_property, has_value, has_length, has_item

from river.config import app_config
from river.hooking.backends.loader import load_callback_backend
from river.hooking.transition import PostTransitionHooking
from river.models.callback import Callback
from river.models.factories import WorkflowFactory, TransitionApprovalMetaFactory, StateObjectFactory, PermissionObjectFactory
from river.tests.models import BasicTestModel
from river.tests.models.factories import BasicTestModelObjectFactory

__author__ = 'ahmetdal'


def test_callback(*args, **kwargs):
    pass


# noinspection DuplicatedCode
class DatabaseHookingBackendTest(TestCase):
    def setUp(self):
        self.field_name = "my_field"
        authorized_permission = PermissionObjectFactory()

        state1 = StateObjectFactory(label="state1")
        state2 = StateObjectFactory(label="state2")

        content_type = ContentType.objects.get_for_model(BasicTestModel)
        workflow = WorkflowFactory(initial_state=state1, content_type=content_type, field_name="my_field")
        TransitionApprovalMetaFactory.create(
            workflow=workflow,
            source_state=state1,
            destination_state=state2,
            priority=0,
            permissions=[authorized_permission]
        )

        app_config.HOOKING_BACKEND_CLASS = 'river.hooking.backends.database.DatabaseHookingBackend'
        self.handler_backend = load_callback_backend()
        self.handler_backend.callbacks = {}

    def test_shouldRegisterAHooking(self):
        workflow_objects = BasicTestModelObjectFactory.create_batch(2)

        hooking_hash = '%s.%s_object%s_field_name%s' % (PostTransitionHooking.__module__, PostTransitionHooking.__name__, workflow_objects[1].pk, self.field_name)

        assert_that(self.handler_backend.callbacks, is_not(has_key(hooking_hash)))

        self.handler_backend.register(PostTransitionHooking, test_callback, workflow_objects[1], self.field_name)

        assert_that(self.handler_backend.callbacks, has_key(hooking_hash))
        assert_that(self.handler_backend.callbacks, has_value(has_property("__name__", test_callback.__name__)))

        self.handler_backend.register(PostTransitionHooking, test_callback, workflow_objects[1], self.field_name)

    def test_shouldRegisterAHookingResilientlyToMultiProcessing(self):
        workflow_objects = BasicTestModelObjectFactory.create_batch(2)

        from multiprocessing import Process, Queue

        assert_that(Callback.objects.all(), has_length(0))

        self.handler_backend.register(PostTransitionHooking, test_callback, workflow_objects[1], self.field_name)

        assert_that(Callback.objects.all(), has_length(1))

        def worker2(q):
            second_handler_backend = load_callback_backend()
            handlers = second_handler_backend.get_callbacks(PostTransitionHooking, workflow_objects[1], self.field_name)
            q.put([f.__name__ for f in handlers])

        q = Queue()
        p2 = Process(target=worker2, args=(q,))

        p2.start()

        handlers = q.get(timeout=1)

        assert_that(handlers, has_length(1))
        assert_that(handlers, has_item(test_callback.__name__))

    def test_shouldReturnTheRegisteredHooking(self):
        workflow_objects = BasicTestModelObjectFactory.create_batch(2)

        self.handler_backend.register(PostTransitionHooking, test_callback, workflow_objects[1], self.field_name)
        handlers = self.handler_backend.get_callbacks(PostTransitionHooking, workflow_objects[1], self.field_name)
        assert_that(handlers, has_length(1))
        assert_that(handlers, has_item(has_property("__name__", test_callback.__name__)))

    def test_get_handlers_in_multiprocessing(self):
        workflow_objects = BasicTestModelObjectFactory.create_batch(2)

        from multiprocessing import Process, Queue

        Callback.objects.update_or_create(
            hash='%s.%s_object%s_field_name%s' % (PostTransitionHooking.__module__, PostTransitionHooking.__name__, workflow_objects[1].pk, self.field_name),
            defaults={
                'method': '%s.%s' % (test_callback.__module__, test_callback.__name__),
                'hooking_cls': '%s.%s' % (PostTransitionHooking.__module__, PostTransitionHooking.__name__),
            }
        )

        def worker2(q):
            handlers = self.handler_backend.get_callbacks(PostTransitionHooking, workflow_objects[1], self.field_name)
            q.put([f.__name__ for f in handlers])

        q = Queue()
        p2 = Process(target=worker2, args=(q,))

        p2.start()

        handlers = q.get(timeout=1)
        assert_that(handlers, has_length(1))
        assert_that(handlers, has_item(test_callback.__name__))