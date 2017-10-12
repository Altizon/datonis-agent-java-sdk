package io.datonis.examples;

import io.datonis.sdk.EdgeGateway;
import io.datonis.sdk.InstructionHandler;
import io.datonis.sdk.Thing;
import io.datonis.sdk.exception.EdgeGatewayException;
import io.datonis.sdk.exception.IllegalThingException;
import io.datonis.sdk.message.AlertType;
import io.datonis.sdk.message.Instruction;

import java.util.Random;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sample program that sends events to Datonis
 * Also showcases executing instructions for file download
 * 
 * @author Rajesh Jangam (rajesh_jangam@altizon.com)
 */
public class SampleAgentWithDownload
{
    private static final Logger logger = LoggerFactory.getLogger(SampleAgent.class);
    private static int NUM_EVENTS = 5;

    private Thing thing;
    private EdgeGateway gateway;

    private SampleAgentWithDownload()
    {
        // The EdgeGateway class is the main class for sending data to Datonis. Create an instance and store it for
        // future use.
        this.gateway = new EdgeGateway();
    }

    public static void main(String[] args) throws IllegalThingException
    {
        // First make sure you create an datonis-edge.properties file using your access and secret keys.
        // A sample file should be available with this package
        // The keys are available in the Datonis platform portal

        logger.info("Starting the Sample Datonis Agent (Download example)");
        // Create and Initialize the agent
        SampleAgentWithDownload agent = new SampleAgentWithDownload();

        // Start the Gateway
        if (!agent.startGateway())
        {
            logger.info("Could not start the agent. Exiting");
            return;
        }

        logger.info("Agent started Successfully");

        // Setup bidirectional communication - Needs to use MQTTS/MQTT mode for communication.
        // Ensure that the 'protocol' property is set to 'mqtts' in the datonis-edge.properties file
        // If you call this when 'protocol' is 'http' or 'https', it will be a no-op
        agent.setupBiDirectionalCommunication();

        // Send a few sample simulated data events
        logger.info("Transmitting Data");
        agent.transmitData();

        // Transmit a set of sample alerts.
        logger.info("Transmitting Alerts");
        agent.transmitAlerts();

        // Transmit a waypoint
        logger.info("Transmitting a waypoint");
        agent.transmitWaypoint();

        logger.info("Exiting");
        agent.stopGateway();
    }

    private boolean startGateway()
    {
        try
        {

            // Create a Thing Object.It is extremely important use a proper Thing key or collisions may occur and data
            // could get overwritten.
            // Please create a Thing on the Datonis portal and use the key here. Thing can be non-unique, however
            // uniqueness is recommended.
            thing = new Thing("<Your Thing Key>", "LivingRoom", "The living room temperature and humidity device.");

            // You can register multiple things and send data for them. First add the things and then call register.
            // In this case, there is only a single thing object so only a single Thing gets added.
            gateway.addThing(thing);

            // Now start the gateway which automatically registers all the Things. You are now ready to transmit data.
            gateway.start();
        }
        catch (IllegalThingException e)
        {
            logger.error("Could not start the edge gateway: ", e.getMessage(), e);
            return false;
        }
        return true;
    }

    private void setupBiDirectionalCommunication()
    {
        // Register an instruction handler with the gatweay. Whenever Datonis sends an instruction down, the instruction
        // handler is automatically invoked.
        gateway.setInstructionHandler(new InstructionHandler() {

            @Override
            public void handleInstructionExecution(EdgeGateway gateway, Instruction instruction)
            {

                logger.info("Received instruction for thing: " + instruction.getThingKey() + " from Datonis: " + instruction.getInstruction().toJSONString());
                // The instruction.getInstruction() call gives details of the passed instruction. You can now take the
                // appropriate action based on that instruction. `
                
                JSONObject instructionCode = instruction.getInstructionBody();
                String command = (String)instructionCode.get("command");
                if (command != null && command.equalsIgnoreCase("download")) {
                    String path = (String)instructionCode.get("path");
                    // Workaround till the path escaping issue gets fixed
                    path = path.replaceAll("\\|", "/");
                    
                    logger.info("Got instruction to download file: " + path + " from the platform");
                    
                    JSONObject data = new JSONObject();
                    try {
                        // Note replace the second argument with your local path where you want the file to be downloaded
                        gateway.downloadFileUsingSftp(path, "/data/play");
                        data.put("execution_status", "success");
                        gateway.transmitAlert(instruction.getAlertKey(), instruction.getThingKey(), AlertType.INFO, "File download successful", data);
                    } catch (EdgeGatewayException e) {
                        data.put("execution_status", "failed");
                        gateway.transmitAlert(instruction.getAlertKey(), instruction.getThingKey(), AlertType.ERROR, "File download failed", data);
                    }
                } else {
                    logger.error("Some unknown command received");
                    JSONObject data = new JSONObject();
                    data.put("execution_status", "failed");
                    gateway.transmitAlert(instruction.getAlertKey(), instruction.getThingKey(), AlertType.ERROR, "Unhandled instruction", data);
                }
            }
        });
    }

    private int generateValue(int min, int max)
    {
        return new Random().nextInt(max - min + 1) + min;

    }

    private void transmitData() throws IllegalThingException
    {
        for (int count = 1; count <= NUM_EVENTS; count++)
        {
            // Construct the JSON packet to be sent. This has to correspond to the metric names. For instance, you have
            // created a ThingTemplate with two metrics, temperature and humidity. You are now sending metrics for the
            // Things that belong to that Thing Template.The metric names that you use here must match the metric names
            // that you have set up in the Thing Template.
            // The format of the data is JSON. We thus build a JSON object.
            JSONObject data = new JSONObject();

            // Put a random value between 0 to 35 for temperature
            data.put("temperature", generateValue(0, 35));
            // Put a random value between 50 to 95 for humidity
            data.put("humidity", generateValue(50, 95));

            // Compress and transmit the data. Compressing is highly recommended for efficient transmit.
            if (!gateway.transmitCompressedData(thing, data, null))
            {
                logger.warn("Could not transmit packet : " + count + " value " + data.toJSONString());
            }

            try
            {
                Thread.currentThread().sleep(5000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }

        }

    }

    private void transmitAlert(AlertType alertType, String message)
    {
        // You can send additional information along with the alert in a JSON format.
        JSONObject alertParams = new JSONObject();
        alertParams.put("message", message);

        if (!gateway.transmitAlert(thing.getKey(), alertType, "This is an example " + alertType.toString() + " alert!", alertParams))
        {
            logger.error("Could not send example " + alertType.toString() + " alert");
        }
    }

    public void transmitAlerts()
    {
        // You can transmit 4 kinds of Alerts. INFO, WARNING, ERROR and CRITICAL.You can also transmit parameters along
        // with the alert that will provide additional information about why the alert was generated.
        transmitAlert(AlertType.INFO, "Everything seems okay");
        transmitAlert(AlertType.WARNING, "An issue could occur");
        transmitAlert(AlertType.ERROR, "An issue has happened");
        transmitAlert(AlertType.CRITICAL, "A failure has happened");

        // Wait a while to ensure that the alerts are transmitted.
        try
        {
            Thread.currentThread().sleep(5000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    private JSONArray createWaypoint(double latitude, double longitude)
    {
        JSONArray waypoint = new JSONArray();
        waypoint.add(latitude);
        waypoint.add(longitude);
        return waypoint;
    }

    private void transmitWaypoint() throws IllegalThingException
    {
        // Create a waypoint. A waypoint is a series of lat/long locations that a Thing can pass through.Currently we
        // will just create a single waypoint. However you can also send a list of waypoints that a Thing could have
        // passed through
        JSONArray waypoint = createWaypoint(18.52, 73.85);

        if (!gateway.transmitCompressedData(thing, null, waypoint))
        {
            logger.warn("Could not transmit a waypoint");
        }

    }

    private void stopGateway()
    {
        gateway.stop();
    }

}
