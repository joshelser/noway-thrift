/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package joshelser;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import joshelser.ParseBase;
import joshelser.ServiceBase;
import joshelser.thrift.HdfsService;

/**
 */
public class Client implements ServiceBase {
  private static final Logger log = LoggerFactory.getLogger(Client.class);

  private static class Opts extends ParseBase {
    @Parameter(names = {"-s", "--server"}, required = true, description = "Hostname of Thrift server")
    private String server;

    @Parameter(names = {"--port"}, required = false, description = "Port of the Thrift server, defaults to ")
    private int port = DEFAULT_THRIFT_SERVER_PORT;

    @Parameter(names = {"-d", "--dir"}, required = false, description = "HDFS directory to perform `ls` on")
    private String dir = "/";
  }

  public static void main(String[] args) throws Exception {
    final Opts opts = new Opts();

    // Parse the options
    opts.parseArgs(Client.class, args);

    // Open up a socket to the server:port
    TTransport transport = new TSocket(opts.server, opts.port);

    // Setup our thrift client to our custom thrift service
    final HdfsService.Client client = new HdfsService.Client(new TCompactProtocol(transport));

    // Open the transport
    transport.open();

    final CountDownLatch latch = new CountDownLatch(2);
    Runnable notOneway = new Runnable() {

      @Override
      public void run() {
        try {
          latch.countDown();
          latch.await();

          Thread.sleep(10);
          log.info("Running synchronous");
          // Invoke the RPC
          client.send_ls(opts.dir);
          Thread.sleep(1500);
          log.info("Recv synchronous");
          client.recv_ls();
          //client.ls(opts.dir);
//          client.pause(1000);
          log.info("Finished synchronous");
  
          // Print out the result
          //System.out.println("$ ls " + opts.dir + "\n" + response);
        } catch (Exception e) {
          log.error("Crap", e);
        } 
      }
      
    };

    Runnable oneway = new Runnable() {

      @Override
      public void run() {
        try {
          latch.countDown();
          latch.await();

          log.info("running oneway");
          client.pause_oneway(1000);
//          client.ls_oneway("/");
          log.info("Finished oneway");
        } catch (Exception e) {
          log.error("Crap", e);
        }
      }
      
    };

    ExecutorService svc = Executors.newFixedThreadPool(2);
    svc.submit(oneway);
    svc.submit(notOneway);

    svc.shutdown();
    svc.awaitTermination(30, TimeUnit.SECONDS);

    // Close the transport (don't leak resources)
    transport.close();
  }
}
