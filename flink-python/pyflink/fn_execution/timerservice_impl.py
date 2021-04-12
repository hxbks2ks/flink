################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import time
from enum import Enum

from typing import Tuple, Set, List

from pyflink.datastream import TimerService
from pyflink.datastream.timerservice import InternalTimer, K, N, InternalTimerService
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend


class InternalTimerImpl(InternalTimer[K, N]):

    def __init__(self, timestamp: int, key: K, namespace: N):
        self._timestamp = timestamp
        self._key = key
        self._namespace = namespace

    def get_timestamp(self) -> int:
        return self._timestamp

    def get_key(self) -> K:
        return self._key

    def get_namespace(self) -> N:
        return self._namespace

    def __hash__(self):
        result = int(self._timestamp ^ (self._timestamp >> 32))
        result = 31 * result + hash(tuple(self._key))
        result = 31 * result + hash(self._namespace)
        return result

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._timestamp == other._timestamp \
            and self._key == other._key and self._namespace == other._namespace


class TimerOperandType(Enum):
    REGISTER_EVENT_TIMER = 0
    REGISTER_PROC_TIMER = 1
    DELETE_EVENT_TIMER = 2
    DELETE_PROC_TIMER = 3


class InternalTimerServiceImpl(InternalTimerService[N]):
    """
    Internal implementation of InternalTimerService.
    """

    def __init__(self, keyed_state_backend: RemoteKeyedStateBackend):
        self._keyed_state_backend = keyed_state_backend
        self._current_watermark = None
        self.timers = []  # type: List[Tuple[TimerOperandType, InternalTimer]]
        self._exist_timers = set()  # type: Set[Tuple[TimerOperandType, InternalTimer]]

    def current_processing_time(self):
        return int(time.time() * 1000)

    def current_watermark(self):
        return self._current_watermark

    def advance_watermark(self, watermark: int):
        self._current_watermark = watermark

    def register_processing_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        timer = (TimerOperandType.REGISTER_PROC_TIMER, InternalTimerImpl(t, current_key, namespace))
        if timer not in self._exist_timers:
            self._exist_timers.add(timer)
            self.timers.append(timer)

    def register_event_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        timer = (TimerOperandType.REGISTER_EVENT_TIMER,
                 InternalTimerImpl(t, current_key, namespace))
        if timer not in self._exist_timers:
            self._exist_timers.add(timer)
            self.timers.append(timer)

    def delete_processing_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        timer = (TimerOperandType.DELETE_PROC_TIMER, InternalTimerImpl(t, current_key, namespace))
        if timer not in self._exist_timers:
            self._exist_timers.add(timer)
            self.timers.append(timer)

    def delete_event_time_timer(self, namespace: N, t: int):
        current_key = self._keyed_state_backend.get_current_key()
        timer = (TimerOperandType.DELETE_EVENT_TIMER, InternalTimerImpl(t, current_key, namespace))
        if timer not in self._exist_timers:
            self._exist_timers.add(timer)
            self.timers.append(timer)


class TimerServiceImpl(TimerService):
    """
    Internal implementation of TimerService.
    """

    def __init__(self, internal_timer_service: InternalTimerServiceImpl):
        self._internal = internal_timer_service
        self.timers = self._internal.timers

    def current_processing_time(self) -> int:
        return self._internal.current_processing_time()

    def current_watermark(self) -> int:
        return self._internal.current_watermark()

    def advance_watermark(self, wm):
        self._internal.advance_watermark(wm)

    def register_processing_time_timer(self, t: int):
        self._internal.register_processing_time_timer(None, t)

    def register_event_time_timer(self, t: int):
        self._internal.register_event_time_timer(None, t)

    def delete_processing_time_timer(self, t: int):
        self._internal.delete_processing_time_timer(None, t)

    def delete_event_time_timer(self, t: int):
        self._internal.delete_event_time_timer(None, t)
