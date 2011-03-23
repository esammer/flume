package com.cloudera.flume.handler.log4j;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import com.cloudera.flume.handlers.avro.AvroEventAdaptor;
import com.cloudera.flume.handlers.avro.FlumeEventAvroServer;
import com.cloudera.flume.handlers.log4j.Log4JEventAdaptor;

public class Log4jAvroAppender extends AppenderSkeleton {

  private static final int defaultReconnectAttempts = 10;

  private FlumeEventAvroServer client;

  protected String hostname;
  protected int port;
  protected int reconnectAttempts;

  public Log4jAvroAppender() {
    super();

    reconnectAttempts = defaultReconnectAttempts;
  }

  protected void connect() {
    int attempt = 0;

    LogLog.debug("attempting to create an Avro connection");

    while (true) {
      if (reconnectAttempts == 0 || attempt <= reconnectAttempts) {
        LogLog.debug("reconnectAttempts allow:" + reconnectAttempts
            + " attempt:" + attempt);

        client = attemptConnection();

        if (client == null) {
          LogLog.debug("connection failed - sleeping");

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }

          attempt++;
        } else {
          // LogLog.debug("got connection:" + client);

          break;
        }
      } else {
        LogLog
            .warn("Exhausted Avro server connection attempts (reconnectAttempts:"
                + reconnectAttempts
                + " attempt:"
                + attempt
                + "). This appender won't function.");

        break;
      }
    }
  }

  protected FlumeEventAvroServer attemptConnection() {
    LogLog.debug("connecting to Avro server hostname:" + hostname + " port:"
        + port);

    URL url = null;

    try {
      url = new URL("http", hostname, port, "/");
    } catch (MalformedURLException e) {
      LogLog.warn("Unable to create a well-formed URL with hostname:"
          + hostname + " port:" + port, e);
    }

    LogLog.debug("using url:" + url);

    Transceiver transciever = new HttpTransceiver(url);
    FlumeEventAvroServer client = null;

    try {
      client = SpecificRequestor.getClient(FlumeEventAvroServer.class,
          transciever);
    } catch (IOException e) {
      LogLog.warn("Unable to create Avro client", e);
    }

    return client;
  }

  @Override
  public void close() {
    client = null;
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (client == null) {
      connect();
    }

    /*
     * This is not the nicest way to do this. Ideally we'd skip the intermediate
     * object and go from the log4j event directly to the AvroFlumeEvent.
     * -esammer
     */
    client.append(AvroEventAdaptor.convert(new Log4JEventAdaptor(event)));
  }

  public FlumeEventAvroServer getClient() {
    return client;
  }

  public void setClient(FlumeEventAvroServer client) {
    this.client = client;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getReconnectAttempts() {
    return reconnectAttempts;
  }

  public void setReconnectAttempts(int reconnectAttempts) {
    this.reconnectAttempts = reconnectAttempts;
  }

}
