package org.robobario.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.robobario.service.Impression;
import org.robobario.service.KafkaGroupedEvents;

import java.util.Collection;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/dsp_event")
@Produces(MediaType.APPLICATION_JSON)
public class DspEventResource {

    private KafkaGroupedEvents events;

    private ObjectMapper mapper;


    public DspEventResource(KafkaGroupedEvents events, ObjectMapper objectMapper) {
        this.events = events;
        this.mapper = objectMapper;
    }


    @GET
    @Path("/latest")
    public ObjectNode getLatest() {
        Impression impr = events.getLatest();
        return toJson(impr);
    }


    @GET
    @Path("/{impressionId}")
    public ObjectNode get(@PathParam("impressionId") String impressionId) {
        Impression impr = events.getImpression(impressionId);
        return toJson(impr);
    }


    @GET
    @Path("/{impressionId}/more")
    public ObjectNode getMore(@PathParam("impressionId") String impressionId) {
        boolean hasNext = events.hasNext(impressionId);
        ObjectNode node = mapper.createObjectNode();
        node.put("more", hasNext);
        return node;
    }


    @GET
    @Path("/{impressionId}/next")
    public ObjectNode getNext(@PathParam("impressionId") String impressionId) {
        Impression impr = events.getNext(impressionId);
        return toJson(impr);
    }


    @GET
    @Path("/{impressionId}/previous")
    public ObjectNode getPrevious(@PathParam("impressionId") String impressionId) {
        Impression impr = events.getPrevious(impressionId);
        return toJson(impr);
    }


    private ObjectNode toJson(Impression latest) {
        ObjectNode node = mapper.createObjectNode();
        ArrayNode impressions = node.putArray("impressions");
        if (latest != null) {
            ObjectNode nodes = impressions.addObject();
            nodes.put("impressionId", latest.getKey());
            Collection<GenericRecord> value = latest
                .getValue()
                .stream()
                .sorted((a, b) -> a.get("dealId").toString().compareTo(b.get("dealId").toString()))
                .collect(Collectors.toList());
            ArrayNode events = nodes.putArray("events");
            for (GenericRecord genericRecord : value) {
                ObjectNode objectNode = events.addObject();
                Schema schema = genericRecord.getSchema();
                for (Schema.Field field : schema.getFields()) {
                    String name = field.name();
                    Object o = genericRecord.get(field.pos());
                    if (o == null) {
                        objectNode.putNull(name);
                    }
                    else {
                        objectNode.put(name, o.toString());
                    }
                }
            }
        }
        return node;
    }
}
