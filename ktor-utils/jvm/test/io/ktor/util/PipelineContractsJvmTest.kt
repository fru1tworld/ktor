/*
 * Copyright 2014-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.util

import io.ktor.util.pipeline.*
import kotlinx.coroutines.*
import java.util.concurrent.LinkedBlockingQueue
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.startCoroutine
import kotlin.test.Test
import kotlin.test.assertNull
import kotlin.test.assertSame

class PipelineContractsJvmTest {

    // Regression test for KTOR-2644.
    @Test
    fun `proceed resumes on the original dispatcher thread after cross-thread suspension`() {
        val phase = PipelinePhase("test")
        val dispatcher = ExternalEventLoopDispatcher()

        dispatcher.runEventLoop {
            val loopThread = Thread.currentThread()
            var resumedThread: Thread? = null
            val pipeline = Pipeline<Unit, Unit>(phase)

            pipeline.intercept(phase) {
                proceed()
                resumedThread = Thread.currentThread()
            }

            // Suspend unintercepted and resume from a raw background thread, so that
            // SFG.continuation.resumeWith — and therefore resumeRootWith — is called
            // from a non-event-loop thread. Without the fix, proceed() would return on
            // that background thread instead of being dispatched back to the event loop.
            pipeline.intercept(phase) {
                suspendCoroutineUninterceptedOrReturn<Unit> { cont ->
                    Thread { cont.resumeWith(Result.success(Unit)) }.start()
                    COROUTINE_SUSPENDED
                }
            }

            pipeline.execute(Unit, Unit)

            assertSame(loopThread, resumedThread, "proceed() returned on wrong thread after cross-thread suspension")
        }
    }

    // Regression test for KTOR-9431.
    @Test
    fun `thread local is restored after proceed followed by cross-thread suspension`() {
        val threadLocal = ThreadLocal<String?>()
        val phase = PipelinePhase("test")
        val dispatcher = ExternalEventLoopDispatcher()

        dispatcher.runEventLoop {
            val pipeline = Pipeline<Unit, Unit>(phase)

            pipeline.intercept(phase) {
                withContext(threadLocal.asContextElement("set")) {
                    proceed()
                }
            }

            pipeline.intercept(phase) {
                withContext(Dispatchers.Default) {}
            }

            pipeline.execute(Unit, Unit)

            // Checked after pipeline.execute() — not inside the interceptor — because
            // UndispatchedCoroutine.afterResume() masks the leak within the pipeline body.
            assertNull(
                threadLocal.get(),
                "SFG leaked thread local after pipeline"
            )
        }
    }
}

// Mimics a Netty/Vert.x event loop: isDispatchNeeded=false on the loop thread, but NOT a
// kotlinx.coroutines EventLoopImplBase, so executeUnconfined runs continuations inline.
private class ExternalEventLoopDispatcher : CoroutineDispatcher() {
    @Volatile private var loopThread: Thread? = null
    private val queue = LinkedBlockingQueue<Runnable>()

    override fun isDispatchNeeded(context: CoroutineContext): Boolean =
        Thread.currentThread() !== loopThread

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        queue.put(block)
    }

    fun runEventLoop(block: suspend () -> Unit) {
        loopThread = Thread.currentThread()
        try {
            var done = false
            var failure: Throwable? = null
            block.startCoroutine(
                Continuation(this) { result ->
                    failure = result.exceptionOrNull()
                    done = true
                    queue.put(Runnable { }) // unblock queue.take() if called from another thread
                }
            )
            while (!done) queue.take().run()
            failure?.let { throw it }
        } finally {
            loopThread = null
        }
    }
}
