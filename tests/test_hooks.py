from typing import Dict, Callable

import datacode as dc
import datacode.hooks as dc_hooks

from tests.pipeline.base import PipelineTest

COUNTER = 0


def increase_counter_hook(**kwargs):
    global COUNTER
    COUNTER += 1
    return kwargs


class HooksTest(PipelineTest):
    hook_attrs = [key for key in dir(dc_hooks) if not key.startswith('_')]
    orig_hook_dict: Dict[str, Callable]

    def setUp(self) -> None:
        super().setUp()
        self.orig_hook_dict = {key: getattr(dc_hooks, key) for key in self.hook_attrs}

    def teardown_method(self, *args, **kwargs):
        super().teardown_method(*args, **kwargs)
        # Reset hooks
        for attr in self.hook_attrs:
            setattr(dc_hooks, attr, self.orig_hook_dict[attr])


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