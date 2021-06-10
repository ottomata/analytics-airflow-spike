import jpype
import jpype.imports
from jpype.types import *

import logging


from jpype_utils import *

# TODO: can this be parameterized? Will have to figure out what happens if we need to change
# jar versions. I guess we'd have to restart airflow scheduler to create a new jvm with a new classpath.
# Unless we can make jpype set  the classpath while the jvm is running?

# classpath = ['/srv/deployment/analytics/refinery/artifacts/refinery-job.jar']
classpath = ['/Users/otto/Projects/wm/analytics/event-utilities/eventutilities/target/eventutilities-1.0.6-SNAPSHOT-shaded.jar']

def get_event_streams(
    stream_names=None,
    stream_settings={'canary_events_enabled': 'true'},
):
    """
    Returns event streams matching stream names and stream config setttings.
    """
    startJVM(classpath=classpath)
    from org.wikimedia.eventutilities.core.event import WikimediaExternalDefaults

    stream_names = list_to_arraylist(stream_names)
    stream_settings = dict_to_hashmap(stream_settings)

    event_stream_factory = WikimediaExternalDefaults.EVENT_STREAM_FACTORY
    return event_stream_factory.createEventStreamsMatchingSettings(
        stream_names,
        stream_settings
    )

def get_canary_event_producer():
    """
    Gets a CanaryEventsProducer.
    TODO: parameterize
    """
    startJVM(classpath=classpath)
    from org.wikimedia.eventutilities.core.event import WikimediaExternalDefaults
    from org.wikimedia.eventutilities.monitoring import CanaryEventProducer;

    return CanaryEventProducer(
        WikimediaExternalDefaults.EVENT_STREAM_FACTORY,
        WikimediaExternalDefaults.WIKIMEDIA_HTTP_CLIENT
    )

def produce_canary_events(
    canary_event_producer,
    stream_names,
    dry_run=False
):
    """
    Produces canary events for stream_names using canary_event_producer.
    Python implementation of
    https://github.com/wikimedia/analytics-refinery-source/blob/master/refinery-job/src/main/scala/org/wikimedia/analytics/refinery/job/ProduceCanaryEvents.scala#L282-L327
    """
    startJVM(classpath=classpath)

    event_streams = get_event_streams(stream_names=stream_names, stream_settings={})

    # Map of event service URI -> List of canary events to produce to that event service.
    uri_to_canary_events = canary_event_producer.getCanaryEventsToPostForStreams(event_streams)

    # Build a description string of the POST requests and events for logging.
    post_description = ''

    for uri, canary_events in uri_to_canary_events.items():
        post_description += f"\nPOST {uri}\n  {canary_events}\n"

    dry_run_message = "DRY-RUN, would have produced " if dry_run else "Producing "

    logging.info(
        dry_run_message +
        "canary events for streams:\n  " +
        "\n  ".join([str(es.streamName()) for es in event_streams]) + "\n" +
        post_description + "\n"
    )

    if not dry_run:
        results = canary_event_producer.postEventsToUris(uri_to_canary_events)

        failures = {
            str(uri): http_result for (uri, http_result) in results.items()
            if not http_result.getSuccess()
        }

        if len(failures) == 0:
            logging.info("All canary events successfully produced.");
        else:
            failures_description = ''
            for uri, http_result in failures.items():
                failures_description += f"POST {uri} => {http_result}. Response body:\n  {http_result.getBodyAsString()}\n\n"
            loggng.error(f'Some canary events failed to be produced:\n{failures_description}');

        return len(failures) == 0
    else:
        return True
