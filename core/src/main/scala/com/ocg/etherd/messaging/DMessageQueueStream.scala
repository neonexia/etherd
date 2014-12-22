package com.ocg.etherd.messaging

import com.ocg.etherd.streams.{WriteableEventStream, ReadableEventStream}

trait DMessageQueueStream extends ReadableEventStream with WriteableEventStream {

}