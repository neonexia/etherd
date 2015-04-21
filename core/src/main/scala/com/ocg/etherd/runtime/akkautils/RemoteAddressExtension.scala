package com.ocg.etherd.runtime.akkautils

import akka.actor.{Extension, ExtensionKey, ExtendedActorSystem }

/**
 * val masterActorUrl = masterActor.path.toStringWithAddress(RemoteAddressExtension(masterSystem).address)
 */
class RemoteAddressExtensionImpl(system: ExtendedActorSystem) extends Extension {
  def address = system.provider.getDefaultAddress
}

object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtensionImpl]


