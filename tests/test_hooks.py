import datacode as dc
import datacode.hooks as dc_hooks

from tests.pipeline.base import PipelineTest

COUNTER = 0


def increase_counter_hook(**kwargs):
    global COUNTER
    COUNTER += 1
    return kwargs


class HooksTest(PipelineTest):

    def teardown_method(self, *args, **kwargs):
        super().teardown_method(*args, **kwargs)
        # Reset hooks
        hook_attrs = [key for key in dir(dc_hooks) if not key.startswith('_')]
        for attr in hook_attrs:
            setattr(dc_hooks, attr, dc_hooks._null_hook)


class TestPipelineHooks(HooksTest):

    def test_no_pipeline_hook(self):
        counter_value = COUNTER
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value

    def test_on_begin_execute_pipeline(self):
        counter_value = COUNTER
        dc_hooks.on_begin_execute_pipeline = increase_counter_hook
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 1

    def test_on_end_execute_pipeline(self):
        counter_value = COUNTER
        dc_hooks.on_end_execute_pipeline = increase_counter_hook
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 1