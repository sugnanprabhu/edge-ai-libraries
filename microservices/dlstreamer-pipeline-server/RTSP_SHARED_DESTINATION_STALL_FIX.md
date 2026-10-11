# RTSP shared destination stall: root cause and fix

**Status:** Fixed on branch `feature/fix-rtsp-shared-destination-stall`
**File changed:** `src/server/rtsp/gstreamer_rtsp_destination.py`

## Summary

`GStreamerRtspDestination` is a single object shared by every RTSP client
connected to a given pipeline path (`GStreamerRtspFactory` is configured with
`set_shared(True)`, and `GStreamerRtspServer` keeps one `Stream` entry per
path, not per client). Its `_push_buffer()` method treated **any** non-`OK`
`Gst.FlowReturn` returned by `appsrc.emit('push-buffer', ...)` as a fatal
error and called `_end_stream()`, which **permanently** clears the shared
`self._app_src` reference and `self._need_data` flag.

Because the underlying shared GStreamer media is not necessarily torn down
and recreated just because one `push-buffer` call returned non-`OK`, this
left the destination permanently unable to deliver frames: new RTSP clients
would still complete `DESCRIBE`/`SETUP`/`PLAY` successfully (the cached media
object is still "alive" from the RTSP server's point of view), but would
never receive any RTP data. The pipeline server's REST API could also
eventually hang (e.g. a `DELETE /pipelines/{id}` call blocking indefinitely),
requiring a full container restart to recover.

## Original issue (as observed)

While load-testing an RTSP consumer against a DL Streamer Pipeline Server
instance emitting MJPEG-over-RTSP (object detection + VLM captioning
pipeline), repeated/concurrent client connect-disconnect cycles eventually
caused:

1. New RTSP clients to successfully complete the RTSP handshake (TCP
   connect, `DESCRIBE` returning valid SDP, `SETUP`/`PLAY` returning `200
   OK`), but then receive **zero RTP packets** - `ffmpeg`/`ffprobe` would
   report `Failed reading RTSP data: Connection timed out` despite the
   handshake succeeding.
2. Once triggered, the stall was permanent for that RTSP path - it did not
   recover on its own and affected all subsequent client connections to the
   same path.
3. In one occurrence, the pipeline-server's own REST API became
   unresponsive (a blocking `DELETE /pipelines/{id}` call never returned;
   subsequent `GET /pipelines/status` calls also hung), requiring
   `docker restart dlstreamer-pipeline-server` to recover.

## Root cause (code-level)

```python
def _push_buffer(self, buffer):
    ...
    retval = self._app_src.emit('push-buffer', buffer)
    if retval != Gst.FlowReturn.OK:
        self._logger.debug(
            "Push buffer failed for stream {} with {}".format(self._rtsp_path, retval))
        self._end_stream()            # <-- unconditionally fatal

def _end_stream(self):
    self._need_data = False
    if self._app_src:
        self._app_src.end_of_stream()
        del self._app_src
        self._app_src = None          # <-- permanently disables the shared destination
```

`Gst.FlowReturn.FLUSHING` is a normal, **transient** return value that
GStreamer elements can produce during ordinary backpressure or pipeline
state changes - it does not mean the appsrc or destination is broken. The
pre-fix code did not distinguish it from genuinely fatal returns (`ERROR`,
`NOT_NEGOTIATED`, `NOT_LINKED`, etc.), so a single transient `FLUSHING`
event permanently disabled the shared destination:

- `_process_frame()` checks `if self._need_data:` before ever attempting
  another `push-buffer` call. Once `_end_stream()` runs, `self._need_data`
  is `False` and `self._app_src` is `None`, and nothing in the normal frame
  path ever sets `self._need_data` back to `True` again (that only happens
  via the `need-data` GObject signal firing on a *new* appsrc instance).
- A new appsrc instance (and the associated `need-data`/`enough-data` signal
  rebinding) is only created when GstRtspServer calls
  `GStreamerRtspFactory.do_create_element()` again for that path. With
  `set_shared(True)`, this only happens when GstRtspServer fully destroys
  and recreates the cached shared media for that path - which does not
  happen just because one client's `push-buffer` call hit `FLUSHING`.
- Result: the destination silently drops every subsequent frame
  (`_process_frame`'s `else` branch just updates a timestamp and does
  nothing else) with no automatic recovery path.

This is why the symptom is RTSP-handshake-succeeds-but-no-data-ever-arrives,
and why it persisted across new client connections until the container was
restarted.

## How to reproduce (minimal setup)

You do not need the full UAV/mavlink stack to reproduce or test this -  any
DL Streamer Pipeline Server instance with an RTSP destination is sufficient.

1. Start `dlstreamer-pipeline-server` with RTSP enabled
   (`ENABLE_RTSP=true`, `RTSP_PORT=8555` or similar) and launch any pipeline
   whose `destination.frame` includes `{"type": "rtsp", "path": "<name>"}`.
2. Confirm a normal client can read it:
   ```bash
   ffprobe -v error -rtsp_transport tcp \
     -i rtsp://<host>:8555/<name> \
     -show_entries stream=codec_name,width,height -of default=noprint_wrappers=1
   ```
3. Start one long-lived "control" client to observe continuous delivery:
   ```bash
   ffmpeg -nostdin -loglevel info -rtsp_transport tcp \
     -i rtsp://<host>:8555/<name> -f null - > control.log 2>&1 &
   ```
4. Generate concurrent client churn with abrupt disconnects (no RTSP
   `TEARDOWN`) to increase the chance of hitting a transient `FLUSHING`
   return on `push-buffer`:
   ```bash
   for round in $(seq 1 15); do
     PIDS=""
     for i in 1 2 3 4 5 6; do
       ffmpeg -nostdin -loglevel error -rtsp_transport tcp \
         -i rtsp://<host>:8555/<name> -f null - < /dev/null &
       PIDS="$PIDS $!"
     done
     sleep 1.0
     for p in $PIDS; do kill -9 "$p" 2>/dev/null; done
     for p in $PIDS; do wait "$p" 2>/dev/null; done
   done
   ```
5. Immediately after, try a fresh client again (step 2's command). On the
   unpatched code, under the right timing this will complete the RTSP
   handshake but hang waiting for data (`Failed reading RTSP data:
   Connection timed out`), and the long-lived control client's frame
   counter will also stop advancing from the point of the stall onward.

Note: because the trigger is a transient `FLOWRETURN` race rather than a
deterministic single action, reproduction timing can vary; sustained
concurrent connect/disconnect churn (as above) raises the probability of
hitting it. A clean single-client abrupt disconnect is not, by itself,
reliably sufficient to reproduce it.

## The fix

```diff
         retval = self._app_src.emit('push-buffer', buffer)
+        if retval == Gst.FlowReturn.FLUSHING:
+            # Transient condition (expected during backpressure/pipeline
+            # state changes). Skip this frame only - do NOT tear down the
+            # shared destination, since a single transient FLUSHING return
+            # should not permanently stop delivery for all clients.
+            self._logger.debug(
+                "Push buffer flushing for stream {}, skipping frame".format(self._rtsp_path))
+            return
         if retval != Gst.FlowReturn.OK:
             self._logger.debug(
                 "Push buffer failed for stream {} with {}".format(self._rtsp_path, retval))
             self._end_stream()
```

- `FLUSHING` now skips the single affected frame and returns, leaving
  `self._app_src`/`self._need_data` untouched so the shared destination
  keeps serving all current and future clients normally.
- Genuinely fatal `FlowReturn`s (`ERROR`, `NOT_NEGOTIATED`, `NOT_LINKED`,
  etc.) are unchanged - `_end_stream()` still runs for those, preserving the
  original behavior for real pipeline breakage.
- The change is purely additive (8 lines) and scoped to a single branch
  inside `_push_buffer()`; no other method, signal binding, or RTSP
  mount-point logic is touched.

### Verification performed

With the fix applied and rebuilt into the running pipeline-server image, the
reproduction steps above were re-run at increased intensity (15 rounds x 6
concurrent clients = 90 abrupt `SIGKILL` disconnects within ~15 seconds)
while two long-lived control clients streamed continuously:

| Check | Result |
|---|---|
| Control client A (~4m34s span) | 5,058 frames delivered, zero errors, zero stalls |
| Control client B (overlapping, ~31s span) | 576 frames delivered, zero errors, zero stalls |
| Fresh client connect immediately after the stress burst | Succeeds immediately, correct codec/resolution |
| Pipeline-server REST API (`/pipelines/status`) | Responsive throughout, no hang |

No stall was observed post-fix under load substantially heavier than the
original incident.
