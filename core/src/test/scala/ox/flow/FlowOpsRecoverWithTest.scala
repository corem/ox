package ox.flow

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*
import ox.channels.ChannelClosedException

class FlowOpsRecoverWithTest extends AnyFlatSpec with Matchers:

  behavior of "Flow.recoverWith"

  it should "pass through elements when upstream flow succeeds" in :
    // given
    val flow = Flow.fromValues(1, 2, 3)
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException =>
        Flow.fromValues(42)
    }

    // when
    val result = flow.recoverWith(recoveryFunction).runToList()

    // then
    result shouldBe List(1, 2, 3)

  it should "emit recovery flow elements when upstream flow fails with handled exception" in :
    // given
    val exception = new IllegalArgumentException("test error")
    val flow = Flow.fromValues(1, 2).concat(Flow.failed(exception))
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException =>
        Flow.fromValues(42, 43)
    }

    // when
    val result = flow.recoverWith(recoveryFunction).runToList()

    // then
    result shouldBe List(1, 2, 42, 43)

  it should "not emit recovery flow when downstream flow fails with handled exception" in :
    // given
    val exception = new IllegalArgumentException("test error")
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException =>
        Flow.fromValues(42)
    }
    val flow = Flow.fromValues(1, 2).recoverWith(recoveryFunction).concat(Flow.failed(exception))

    // when & then
    the[IllegalArgumentException] thrownBy {
      flow.runToList()
    } should have message "test error"

  it should "propagate unhandled exceptions" in :
    // given
    val exception = new RuntimeException("unhandled error")
    val flow = Flow.fromValues(1, 2).concat(Flow.failed(exception))
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException =>
        Flow.fromValues(42)
    }

    // when & then
    val caught = the[ChannelClosedException.Error] thrownBy {
      flow.recoverWith(recoveryFunction).runToList()
    }
    caught.getCause shouldBe an[RuntimeException]
    caught.getCause.getMessage shouldBe "unhandled error"

  it should "handle multiple exception types" in :
    // given
    val exception = new IllegalStateException("state error")
    val flow = Flow.fromValues(1, 2).concat(Flow.failed(exception))
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException => Flow.fromValues(42)
      case _: IllegalStateException => Flow.fromValues(99, 100)
      case _: NullPointerException => Flow.fromValues(0)
    }

    // when
    val result = flow.recoverWith(recoveryFunction).runToList()

    // then
    result shouldBe List(1, 2, 99, 100)

  it should "work with different recovery value type" in :
    // given
    val exception = new IllegalArgumentException("test error")
    val flow = Flow.fromValues("a", "b").concat(Flow.failed(exception))
    val recoveryFunction: PartialFunction[Throwable, Flow[String]] = {
      case _: IllegalArgumentException =>
        Flow.fromValues("recovered-1", "recovered-2")
    }

    // when
    val result = flow.recoverWith(recoveryFunction).runToList()

    // then
    result shouldBe List("a", "b", "recovered-1", "recovered-2")

  it should "handle exception thrown during flow processing" in :
    // given
    val flow = Flow.fromValues(1, 2, 3).map(x => if x == 3 then throw new IllegalArgumentException("map error") else x)
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException =>
        Flow.fromValues(-1, -2)
    }

    // when
    val result = flow.recoverWith(recoveryFunction).runToList()

    // then
    result shouldBe List(1, 2, -1, -2)

  it should "work with empty flow" in :
    // given
    val flow = Flow.empty[Int]
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException =>
        Flow.fromValues(42)
    }

    // when
    val result = flow.recoverWith(recoveryFunction).runToList()

    // then
    result shouldBe List.empty

  it should "propagate exception when recovery flow throws" in :
    // given
    val originalException = new IllegalArgumentException("original error")
    val flow = Flow.fromValues(1, 2).concat(Flow.failed(originalException))
    val recoveryFunction: PartialFunction[Throwable, Flow[Int]] = {
      case _: IllegalArgumentException =>
        Flow.failed(new RuntimeException("recovery failed"))
    }

    // when & then
    val caught = the[ChannelClosedException.Error] thrownBy {
      flow.recoverWith(recoveryFunction).runToList()
    }
    caught.getCause shouldBe an[RuntimeException]
    caught.getCause.getMessage shouldBe "recovery failed"
end FlowOpsRecoverWithTest
