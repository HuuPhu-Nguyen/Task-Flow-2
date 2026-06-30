package gui;

import transport.BrokerTransport;

interface RabbitMqBrokerConnection extends CoordinatorConnection {
    BrokerTransport transport();

    String peerId();
}
