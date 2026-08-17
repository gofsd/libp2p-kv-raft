package com.gofsd.kvdemo

import android.content.Context
import android.content.SharedPreferences
import android.hardware.camera2.CaptureRequest
import android.util.Log
import android.util.Size
import androidx.camera.camera2.interop.Camera2Interop
import androidx.camera.camera2.interop.ExperimentalCamera2Interop
import androidx.camera.core.Camera
import androidx.camera.core.CameraControl
import androidx.camera.core.CameraSelector
import androidx.camera.core.ExperimentalGetImage
import androidx.camera.core.FocusMeteringAction
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.ImageProxy
import androidx.camera.core.Preview
import androidx.camera.core.TorchState
import androidx.camera.core.resolutionselector.AspectRatioStrategy
import androidx.camera.core.resolutionselector.ResolutionSelector
import androidx.camera.core.resolutionselector.ResolutionStrategy
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.FlashOff
import androidx.compose.material.icons.filled.FlashOn
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Slider
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.BlendMode
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.CompositingStrategy
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.compose.LocalLifecycleOwner
import com.google.zxing.BarcodeFormat
import com.google.zxing.BinaryBitmap
import com.google.zxing.DecodeHintType
import com.google.zxing.MultiFormatReader
import com.google.zxing.NotFoundException
import com.google.zxing.PlanarYUVLuminanceSource
import com.google.zxing.common.HybridBinarizer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

/**
 * How often [MainScannerWidget] forces an explicit refocus -- CONTROL_AF_MODE_CONTINUOUS_PICTURE
 * (set below on both the preview and analysis streams) stops actively re-evaluating focus once it
 * judges the scene already in focus, and a scanning rig with a fixed camera pointed at a fixed
 * screen is exactly the static-scene case that setting is least equipped for: confirmed live, a
 * real device left aimed at an unmoving on-screen code produced *zero* decode activity -- not even
 * the usual short-noise discards -- for minutes at a time. A periodic explicit
 * [CameraControl.startFocusAndMetering] nudge forces AF to re-evaluate on a schedule instead of
 * relying on it to notice a genuinely blurry frame on its own.
 */
private const val REFOCUS_INTERVAL_MS = 3_000L

/**
 * How long [MainScannerWidget] tolerates an analysis stream that has stopped producing *new*
 * imagery before it rebinds the camera from scratch. Two failure modes, one remedy:
 *
 *  - no frames delivered at all, and
 *  - frames still delivered at full rate but every one byte-identical to the last, i.e. the
 *    analysis stream frozen on a stale image while the camera stays bound.
 *
 * Both look exactly like "there is nothing to scan" from outside: the preview keeps painting live
 * (it is a separate CameraX use case and goes on updating normally), nothing is logged, and
 * scanning is simply over. The frozen case is the more deceptive of the two, because the stale
 * frame usually still contains the *previous* code, whose re-decode ScannerCoordinator's own dedup
 * correctly discards -- so even the decode path falls silent. Seen repeatedly on the real
 * two-device rig: a run decoding a code every one to two seconds stops dead mid-run, at a
 * different case each time, while a screenshot of the very same device's preview shows the new
 * code sharp, centred and (confirmed by replaying those frames through this same ZXing pipeline
 * offline) perfectly decodable.
 *
 * Comfortably longer than any legitimate gap: frames arrive continuously at preview rate whether or
 * not a code is in view, and real imagery of a real scene never repeats byte for byte.
 */
private const val FRAME_STALL_TIMEOUT_MS = 10_000L

/** When the analyzer last received a frame whose content differed from the one before it -- see [FRAME_STALL_TIMEOUT_MS]. Written on the decode executor, read from the widget's own coroutine. */
@Volatile
private var lastFrameAtMs = 0L

/** Cheap sampled digest of the previous analyzed frame, for the freshness check behind [FRAME_STALL_TIMEOUT_MS]. */
private var lastFrameDigest = 0L

/**
 * Set by [MainScannerManager.requestRebind] and consumed by [MainScannerWidget]'s watchdog loop --
 * an out-of-band way to ask for the same fresh camera bind [FRAME_STALL_TIMEOUT_MS] triggers on its
 * own, for a caller that has better information than this widget does about whether scanning is
 * actually working (see that function).
 */
@Volatile
private var rebindRequested = false

/**
 * How far below its own best-observed value this camera's frame sharpness has to fall, and for how
 * long, before [MainScannerWidget] treats the focus as lost and rebinds.
 *
 * The failure this exists for, measured by dumping the analyzer's own frames mid-run on the real
 * two-device rig: sharpness sat at 0.017-0.027 for the first minute of a run, then collapsed by
 * roughly 6x to 0.0035 and stayed flat at that value for every remaining frame. The camera was
 * still bound, still delivering fresh frames, and the preview beside it still looked fine -- but
 * nothing decoded again, and neither the periodic refocus nudge nor an explicit
 * [CameraControl.startFocusAndMetering] brought it back. A fresh bind is the only lever left.
 *
 * A ratio rather than an absolute threshold because absolute sharpness depends on the scene, the
 * lighting and what is in frame; the collapse is a property of *this* camera against *its own*
 * recent best, which is what makes it recognisable at all.
 */
private const val FOCUS_LOST_RATIO = 0.4

/** How long sharpness must stay below [FOCUS_LOST_RATIO] of its best before a rebind -- long enough that a code briefly leaving the frame is not mistaken for lost focus. */
private const val FOCUS_LOST_TIMEOUT_MS = 8_000L

/** Exponentially-smoothed sharpness of recent analyzed frames, and the best seen since the last rebind. Written on the decode executor, read from the widget's own coroutine. */
@Volatile
private var recentSharpness = 0.0

@Volatile
private var bestSharpness = 0.0

/** When sharpness was last at or above [FOCUS_LOST_RATIO] of [bestSharpness]. */
@Volatile
private var lastSharpAtMs = 0L

/** Ticks of the watchdog loop, for its heartbeat log. */
private var heartbeatTicks = 0

/**
 * Mean absolute horizontal gradient over a subsample of rows -- a cheap stand-in for focus. A
 * sharp frame of a DataMatrix has hard black/white module edges and scores high; the same frame
 * defocused scores several times lower. Sampled every 8th row and every 2nd pixel, so this reads
 * well under 10% of the plane.
 */
private fun frameSharpness(bytes: ByteArray, width: Int, height: Int): Double {
    var sum = 0L
    var n = 0
    var y = 0
    while (y < height) {
        val row = y * width
        var x = 0
        while (x < width - 2) {
            val a = bytes[row + x].toInt() and 0xFF
            val b = bytes[row + x + 2].toInt() and 0xFF
            sum += if (a > b) (a - b) else (b - a)
            n++
            x += 2
        }
        y += 8
    }
    return if (n == 0) 0.0 else sum.toDouble() / n / 255.0
}

/** True once sharpness has sat below [FOCUS_LOST_RATIO] of its best for [FOCUS_LOST_TIMEOUT_MS]. */
private fun focusLooksLost(): Boolean {
    if (bestSharpness <= 0.0 || lastSharpAtMs == 0L) return false
    return System.currentTimeMillis() - lastSharpAtMs > FOCUS_LOST_TIMEOUT_MS
}

/** Forgets the sharpness baseline, so a freshly-bound camera is judged against what it can do now rather than what the previous bind managed. */
private fun resetSharpnessBaseline() {
    bestSharpness = 0.0
    recentSharpness = 0.0
    lastSharpAtMs = System.currentTimeMillis()
}

/**
 * A sampled digest of one luminance plane -- every 997th byte (a prime, so the stride never lines
 * up with the image's own row width and the samples sweep across the whole frame rather than down
 * one column). ~2700 bytes read out of a 2.7MB plane: enough that two genuinely different camera
 * frames practically never collide, cheap enough to run on every frame.
 */
private fun luminanceDigest(bytes: ByteArray): Long {
    var h = 1469598103934665603L
    var i = 0
    while (i < bytes.size) {
        h = (h xor bytes[i].toLong()) * 1099511628211L
        i += 997
    }
    return h
}

/**
 * The live camera preview + DataMatrix decode loop, ported from
 * object-history-app's identically-shaped widget -- the one difference is
 * [onResult] here delivers the decoded [ByteArray] (via
 * [DataMatrixCodec.resultToBytes]) instead of ZXing's raw decoded String,
 * since a scanned code carries capnp-encoded binary. Assumes camera
 * permission is already granted -- [ScannerHost] is this widget's only
 * caller and never mounts it without checking first.
 */
@OptIn(ExperimentalCamera2Interop::class, ExperimentalGetImage::class)
@Composable
fun MainScannerWidget(
    isExpanded: Boolean,
    onResult: (ByteArray) -> Unit,
    modifier: Modifier = Modifier,
) {
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current
    // FIT_CENTER (letterboxed, whole sensor frame visible), not PreviewView's own default
    // FILL_CENTER (crop-to-fill) -- on the minimized bubble specifically (a small, roughly square
    // container against the camera's wider sensor aspect ratio), FILL_CENTER crops away a large
    // share of the actual frame just to fill that shape, so a code sitting anywhere outside the
    // narrow cropped-in view was invisible to whoever's aiming the device even though
    // ImageAnalysis (a separate CameraX use case, unaffected by Preview's own crop/scale) was
    // still analyzing that full, wider frame regardless -- decode capability was never actually
    // smaller in minimized mode, only what a human could *see* to verify alignment against was.
    // FIT_CENTER makes the two match on any container size/shape.
    val previewView = remember { PreviewView(context).apply { scaleType = PreviewView.ScaleType.FIT_CENTER } }

    var torchOn by remember { mutableStateOf(false) }
    var zoomRatio by remember { mutableStateOf(1f) }
    var maxZoom by remember { mutableStateOf(1f) }
    var minZoom by remember { mutableStateOf(1f) }

    val decodeExecutor = remember { Executors.newSingleThreadExecutor() }
    val analyzerScanning = remember { AtomicBoolean(false) }

    LaunchedEffect(Unit) {
        val cameraProvider = withContext(Dispatchers.Main) {
            ProcessCameraProvider.getInstance(context).get()
        }

        val previewSelector = ResolutionSelector.Builder()
            .setAspectRatioStrategy(AspectRatioStrategy.RATIO_4_3_FALLBACK_AUTO_STRATEGY)
            .build()

        val preview = Preview.Builder()
            .setResolutionSelector(previewSelector)
            .apply {
                Camera2Interop.Extender(this)
                    .setCaptureRequestOption(
                        CaptureRequest.CONTROL_AF_MODE,
                        CaptureRequest.CONTROL_AF_MODE_CONTINUOUS_PICTURE,
                    )
            }
            .build()
            .also { it.setSurfaceProvider(previewView.surfaceProvider) }

        val analysisSelector = ResolutionSelector.Builder()
            .setAspectRatioStrategy(AspectRatioStrategy.RATIO_4_3_FALLBACK_AUTO_STRATEGY)
            .setResolutionStrategy(
                ResolutionStrategy(
                    // 640x480 turned out too coarse for this app's own denser real-world codes:
                    // confirmed live via a device screenshot mid-scan that a code visibly on
                    // screen, in frame and legible to the eye in the full preview, still never
                    // decoded for two full minutes -- at 640x480 a ~30-module-wide DataMatrix that
                    // doesn't fill most of the frame works out to only a handful of analysis
                    // pixels per module, which ZXing needs considerably more of to binarize
                    // reliably.
                    //
                    // 1280x960 in turn was not enough for the densest code this app generates: a
                    // 336-byte signed join ticket is a 144x144-module symbol, and a device seeing
                    // it across a desk fills maybe half the frame with it, which works out to
                    // ~3.6 analysis pixels per module. Measured against this exact decode pipeline
                    // (same HybridBinarizer, same TRY_HARDER hint) on a clean rendering of that
                    // very symbol, ZXing stops decoding below ~2.3 px/module -- so 3.6 leaves no
                    // margin at all for the noise, slight defocus and screen moire a real camera
                    // frame adds on top, and live runs bore that out: the same code that a
                    // 53-93 byte RunCode's ~9 px/module decodes from in ~2s did not decode across
                    // two full 90s exposures.
                    //
                    // 1600x1200 rather than more: it restores the margin that matters (~4.5
                    // px/module in the same geometry) and is the resolution this rig's camera
                    // already runs its own preview stream at, so it is known-good on that HAL.
                    // Going further to 1920x1440 did decode the dense ticket -- in 1.4s, against
                    // never -- but long runs then degraded partway through in a way 1280x960 never
                    // did: the analyzer's frames went soft and stayed soft (dumped frame sharpness
                    // collapsing ~6x mid-run and never recovering), which no amount of refocusing
                    // brought back, while the preview stream beside it stayed sharp.
                    Size(1600, 1200),
                    ResolutionStrategy.FALLBACK_RULE_CLOSEST_HIGHER_THEN_LOWER,
                ),
            )
            .build()

        val imageAnalyzer = ImageAnalysis.Builder()
            .setResolutionSelector(analysisSelector)
            .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
            .apply {
                Camera2Interop.Extender(this)
                    .setCaptureRequestOption(
                        CaptureRequest.CONTROL_AF_MODE,
                        CaptureRequest.CONTROL_AF_MODE_CONTINUOUS_PICTURE,
                    )
            }
            .build()
            .also { analyzer ->
                analyzer.setAnalyzer(decodeExecutor) { imageProxy ->
                    if (analyzerScanning.get()) {
                        imageProxy.close()
                        return@setAnalyzer
                    }
                    analyzerScanning.set(true)
                    // try/finally, not a plain sequence: anything escaping the decode -- an
                    // OutOfMemoryError above all, which is a Throwable this Exception-catching
                    // decode path does not swallow -- would otherwise leave this flag stuck true
                    // and silently end scanning for the rest of the process's life, with the
                    // camera still bound and previewing as if nothing were wrong.
                    try {
                        processImageProxySafe(imageProxy, onResult)
                    } finally {
                        analyzerScanning.set(false)
                    }
                }
            }

        cameraProvider.unbindAll()
        var camera = cameraProvider.bindToLifecycle(
            lifecycleOwner,
            CameraSelector.DEFAULT_BACK_CAMERA,
            preview,
            imageAnalyzer,
        )
        lastFrameAtMs = System.currentTimeMillis()
        // The *resolved* analysis resolution, not the requested one: ResolutionStrategy only
        // expresses a preference, and what a given camera actually hands back is the one number
        // that decides whether a dense code has enough pixels per module to decode at all (see
        // the analysisSelector above). Worth a line in the log, since diagnosing a code that
        // "looks fine on screen but never decodes" otherwise starts from guesswork.
        Log.i("KVDemo", "AUTO: camera bound, scanner live (analysis resolution ${imageAnalyzer.resolutionInfo?.resolution})")

        MainScannerManager.setup(context.applicationContext, camera.cameraControl)

        // The saved zoom can only be re-applied once the camera has reported its real
        // min/max ratio -- restoring before that would clamp against the placeholder 1f..1f
        // range every bind. The first zoom report is that signal, hence the one-shot flag.
        var settingsRestored = false

        fun observeCameraState(bound: Camera) {
            bound.cameraInfo.zoomState.observe(lifecycleOwner) { state ->
                zoomRatio = state.zoomRatio
                minZoom = state.minZoomRatio
                maxZoom = state.maxZoomRatio
                if (!settingsRestored) {
                    settingsRestored = true
                    MainScannerManager.restoreSavedSettings(minZoom, maxZoom)
                }
            }
            bound.cameraInfo.torchState.observe(lifecycleOwner) { state ->
                torchOn = (state == TorchState.ON)
            }
        }
        observeCameraState(camera)

        // See REFOCUS_INTERVAL_MS's own doc comment. Runs for as long as this LaunchedEffect does
        // (the widget's whole lifetime -- cancelled automatically on dispose, same as the analyzer
        // binding above it), centered on the frame since that's where ScanFrameOverlay's own
        // target window is drawn. Doubles as the stall watchdog (see FRAME_STALL_TIMEOUT_MS): the
        // only way back from a camera that has stopped delivering frames is a fresh bind, and the
        // log line is the only evidence anyone gets that it happened at all.
        while (isActive) {
            delay(REFOCUS_INTERVAL_MS)

            // A heartbeat, not a diagnostic left in by accident: everything this loop does (the
            // refocus nudge, the stall watchdog, the rebind request) is silent when working, so a
            // loop that has quietly stopped running is indistinguishable from one with nothing to
            // do -- and a stopped loop means focus is never nudged again, which is exactly the
            // failure this file's other comments describe. One line per 10 ticks is cheap enough
            // to keep and specific enough to answer "was the watchdog even alive?" from a log.
            heartbeatTicks++
            if (heartbeatTicks % 10 == 0) {
                Log.i("KVDemo", "AUTO: scanner watchdog alive (tick $heartbeatTicks, sharpness=${"%.4f".format(recentSharpness)}, best=${"%.4f".format(bestSharpness)})")
            }

            val idleMs = System.currentTimeMillis() - lastFrameAtMs
            if (idleMs > FRAME_STALL_TIMEOUT_MS || rebindRequested || focusLooksLost()) {
                val why = when {
                    idleMs > FRAME_STALL_TIMEOUT_MS -> "no new frame content for ${idleMs}ms"
                    rebindRequested -> "a rebind was requested"
                    else -> "focus looks lost (sharpness ${"%.4f".format(recentSharpness)} vs best ${"%.4f".format(bestSharpness)})"
                }
                rebindRequested = false
                resetSharpnessBaseline()
                Log.w("KVDemo", "AUTO: rebinding the camera -- $why")
                // Explicitly on the main thread, like every other CameraX call in this loop -- see
                // the metering nudge below for what happens when that is merely assumed.
                camera = withContext(Dispatchers.Main) {
                    cameraProvider.unbindAll()
                    val rebound = cameraProvider.bindToLifecycle(
                        lifecycleOwner,
                        CameraSelector.DEFAULT_BACK_CAMERA,
                        preview,
                        imageAnalyzer,
                    )
                    MainScannerManager.setup(context.applicationContext, rebound.cameraControl)
                    // The old cameraInfo's LiveData stops emitting once its camera is unbound, so
                    // the zoom/torch state this widget draws would freeze at whatever it last saw
                    // without re-observing the new one. LiveData.observe is itself main-thread-only.
                    observeCameraState(rebound)
                    rebound
                }
                lastFrameAtMs = System.currentTimeMillis()
                Log.i("KVDemo", "AUTO: camera rebound after stall (analysis resolution ${imageAnalyzer.resolutionInfo?.resolution})")
                continue
            }

            // withContext(Dispatchers.Main), not a bare call: PreviewView.getMeteringPointFactory
            // asserts it is on the main thread, and this coroutine only *usually* is. It resumes on
            // whatever dispatcher the composition it belongs to provides, which is the main thread
            // in the running app but not under Compose's instrumentation test environment, where a
            // frame can be driven from another thread entirely. Measured on the two-device optical
            // rig: this line threw "Not in application's main thread" from a background frame,
            // which killed this LaunchedEffect -- taking the refocus loop, the stall watchdog and
            // (because the failure propagates to the composition's effect scope) AppRoot's scan
            // collector with it, so the app went silently deaf to every later scan. Asking for the
            // main thread costs nothing when already on it and makes the requirement true rather
            // than assumed.
            withContext(Dispatchers.Main) {
            val point = previewView.meteringPointFactory.createPoint(0.5f, 0.5f)
            // No disableAutoCancel, and AE metered alongside AF: the point of this nudge is to
            // make the camera re-evaluate focus, and pinning the result does the opposite. A
            // pinned lock that happens to land blurry -- easy to do, since these triggers fire
            // while device A is mid-transition between one code and the next -- stays blurry, and
            // every later trigger just re-pins from that same bad state with nothing to pull it
            // back. Measured on the real rig by dumping the analyzer's own frames: frame sharpness
            // sat around 0.018-0.027 for the first minute of a run, collapsed ~6x to 0.0035
            // partway through, and then stayed flat at that value for every remaining frame --
            // the camera never recovered, and the run's remaining cases could not decode a code
            // that a screenshot of the same device's preview showed plainly. Letting the action
            // auto-cancel hands 3A back to CONTROL_AF_MODE_CONTINUOUS_PICTURE between nudges, so a
            // bad lock lasts until the next frame instead of the rest of the process.
            val action = FocusMeteringAction.Builder(point, FocusMeteringAction.FLAG_AF or FocusMeteringAction.FLAG_AE).build()
            camera.cameraControl.startFocusAndMetering(action)
            }
        }
    }

    Box(modifier = modifier.fillMaxSize()) {
        AndroidView(factory = { previewView }, modifier = Modifier.fillMaxSize())

        ScanFrameOverlay(
            frameColor = MaterialTheme.colorScheme.primary,
            modifier = Modifier.fillMaxSize(),
            frameSizeFraction = if (isExpanded) 0.62f else 0.68f,
            cornerRadius = if (isExpanded) 24.dp else 8.dp,
            strokeWidth = if (isExpanded) 5.dp else 2.dp,
            bracketFraction = if (isExpanded) 0.16f else 0.22f,
            scrimAlpha = if (isExpanded) 0.55f else 0.3f,
        )

        if (isExpanded) {
            IconButton(
                onClick = { ScannerCoordinator.expanded = false },
                modifier = Modifier
                    .align(Alignment.TopStart)
                    .statusBarsPadding()
                    .padding(16.dp)
                    .size(44.dp)
                    .background(Color.Black.copy(alpha = 0.4f), CircleShape),
            ) {
                Icon(Icons.Filled.Close, contentDescription = "Close scanner", tint = Color.White)
            }

            IconButton(
                onClick = { MainScannerManager.toggleTorch() },
                modifier = Modifier
                    .align(Alignment.TopEnd)
                    .statusBarsPadding()
                    .padding(16.dp)
                    .size(44.dp)
                    .background(Color.Black.copy(alpha = 0.4f), CircleShape),
            ) {
                Icon(
                    if (torchOn) Icons.Filled.FlashOn else Icons.Filled.FlashOff,
                    contentDescription = "Toggle torch",
                    tint = Color.White,
                )
            }

            Row(
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .navigationBarsPadding()
                    .padding(horizontal = 24.dp, vertical = 24.dp)
                    .fillMaxWidth(0.85f)
                    .background(Color.Black.copy(alpha = 0.4f), RoundedCornerShape(28.dp))
                    .padding(horizontal = 16.dp, vertical = 4.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Text(
                    "%.1fx".format(zoomRatio),
                    color = Color.White,
                    style = MaterialTheme.typography.labelLarge,
                    modifier = Modifier.padding(end = 8.dp),
                )
                Slider(
                    value = zoomRatio,
                    onValueChange = { MainScannerManager.setZoom(it) },
                    modifier = Modifier.weight(1f),
                    valueRange = minZoom..maxZoom,
                )
            }
        }
    }
}

/**
 * The viewfinder chrome: a dimmed scrim with a punched-out square window
 * in the middle and four corner brackets around it. Drawn as one Canvas
 * rather than composed out of Boxes because the window is a *hole* --
 * [BlendMode.Clear] only clears what's already been drawn in the same
 * layer, which is why the whole thing renders into an offscreen layer.
 * Every dimension is a fraction of the smaller canvas side, so the same
 * overlay works at both the minimized bubble size and full screen.
 */
@Composable
private fun ScanFrameOverlay(
    frameColor: Color,
    modifier: Modifier = Modifier,
    frameSizeFraction: Float,
    cornerRadius: Dp,
    strokeWidth: Dp,
    bracketFraction: Float,
    scrimAlpha: Float,
) {
    val scrimColor = Color.Black.copy(alpha = scrimAlpha)

    Canvas(
        modifier = modifier.graphicsLayer(
            alpha = 0.99f,
            compositingStrategy = CompositingStrategy.Offscreen,
        ),
    ) {
        val frameSize = size.minDimension * frameSizeFraction
        val cornerRadiusPx = cornerRadius.toPx()
        val left = (size.width - frameSize) / 2f
        val top = (size.height - frameSize) / 2f

        if (scrimAlpha > 0f) {
            drawRect(color = scrimColor)
            drawRoundRect(
                color = Color.Transparent,
                topLeft = Offset(left, top),
                size = androidx.compose.ui.geometry.Size(frameSize, frameSize),
                cornerRadius = CornerRadius(cornerRadiusPx),
                blendMode = BlendMode.Clear,
            )
        }

        val bracketLen = frameSize * bracketFraction
        val strokeWidthPx = strokeWidth.toPx()
        val stroke = Stroke(width = strokeWidthPx, cap = StrokeCap.Round)
        val inset = strokeWidthPx / 2f

        // Top-left
        drawLine(frameColor, Offset(left + inset, top + cornerRadiusPx), Offset(left + inset, top + bracketLen), strokeWidth = stroke.width, cap = stroke.cap)
        drawLine(frameColor, Offset(left + cornerRadiusPx, top + inset), Offset(left + bracketLen, top + inset), strokeWidth = stroke.width, cap = stroke.cap)
        // Top-right
        drawLine(frameColor, Offset(left + frameSize - inset, top + cornerRadiusPx), Offset(left + frameSize - inset, top + bracketLen), strokeWidth = stroke.width, cap = stroke.cap)
        drawLine(frameColor, Offset(left + frameSize - cornerRadiusPx, top + inset), Offset(left + frameSize - bracketLen, top + inset), strokeWidth = stroke.width, cap = stroke.cap)
        // Bottom-left
        drawLine(frameColor, Offset(left + inset, top + frameSize - cornerRadiusPx), Offset(left + inset, top + frameSize - bracketLen), strokeWidth = stroke.width, cap = stroke.cap)
        drawLine(frameColor, Offset(left + cornerRadiusPx, top + frameSize - inset), Offset(left + bracketLen, top + frameSize - inset), strokeWidth = stroke.width, cap = stroke.cap)
        // Bottom-right
        drawLine(frameColor, Offset(left + frameSize - inset, top + frameSize - cornerRadiusPx), Offset(left + frameSize - inset, top + frameSize - bracketLen), strokeWidth = stroke.width, cap = stroke.cap)
        drawLine(frameColor, Offset(left + frameSize - cornerRadiusPx, top + frameSize - inset), Offset(left + frameSize - bracketLen, top + frameSize - inset), strokeWidth = stroke.width, cap = stroke.cap)
    }
}

/** See [processImageProxySafe]'s short-decode discard for why this floor exists. */
private const val MIN_PLAUSIBLE_PAYLOAD_BYTES = 20

// A still-displayed code decodes on nearly every analyzed frame (confirmed live: tens of times a
// second), so logging every single one -- especially across a real-camera optical case's own
// multi-second/multi-minute hold window -- fills logcat's ring buffer with thousands of identical
// lines and evicts the far rarer, actually diagnostic ones (OPTICAL:/ACTION_REQUIRED: lines) before
// pkg/e2erun/android_optical.go's own pullKVDemoLogcat ever gets to read them back. Only the first
// decode of a given payload is genuinely new information -- every later identical one is exactly
// the redundancy ScannerCoordinator.onScanned's own dedup already discards -- so gate this log line
// on the same "did the content actually change" check, kept here (not moved into onScanned itself)
// so a caller that never reaches ScannerCoordinator still gets exactly one log line per distinct
// code, not zero.
private var lastLoggedDecode: ByteArray? = null

/** Reused luminance plane buffer -- see its use in [processImageProxySafe] for why, and for the single-thread invariant that makes it safe. */
private var luminanceBuffer: ByteArray? = null

/** Decodes one camera frame for a DataMatrix code, delivering the original bytes (not ZXing's raw text) via [DataMatrixCodec.resultToBytes]. */
@ExperimentalGetImage
private fun processImageProxySafe(imageProxy: ImageProxy, onResult: (ByteArray) -> Unit) {
    val mediaImage = imageProxy.image
    if (mediaImage == null) {
        imageProxy.close()
        return
    }

    try {
        val width = mediaImage.width
        val height = mediaImage.height
        val yPlane = mediaImage.planes[0]
        val yBuffer = yPlane.buffer
        // Reused across frames rather than allocated per frame: at this analyzer's resolution a
        // luminance plane is ~2.7MB, and a fresh one per frame at preview frame rates is tens of
        // megabytes per second of pure garbage in the same heap the in-process libp2p node is
        // running in. Safe to share without synchronization because every call arrives on the
        // single-threaded decodeExecutor (see its own construction in MainScannerWidget) -- one
        // frame is ever in flight here at a time, which the analyzerScanning gate enforces too.
        val needed = yBuffer.remaining()
        var yuvBytes = luminanceBuffer
        if (yuvBytes == null || yuvBytes.size != needed) {
            yuvBytes = ByteArray(needed)
            luminanceBuffer = yuvBytes
        }
        yBuffer.get(yuvBytes)

        // Freshness, not arrival: a frozen analysis stream keeps delivering frames at full rate
        // (see FRAME_STALL_TIMEOUT_MS), so only a change in the imagery itself counts as this
        // stream still being alive.
        val digest = luminanceDigest(yuvBytes)
        val now = System.currentTimeMillis()
        if (digest != lastFrameDigest) {
            lastFrameDigest = digest
            lastFrameAtMs = now
        }

        // Focus health, tracked per frame and judged by the watchdog -- see FOCUS_LOST_RATIO.
        val sharpness = frameSharpness(yuvBytes, width, height)
        recentSharpness = if (recentSharpness == 0.0) sharpness else recentSharpness * 0.8 + sharpness * 0.2
        if (recentSharpness > bestSharpness) bestSharpness = recentSharpness
        if (lastSharpAtMs == 0L || recentSharpness >= bestSharpness * FOCUS_LOST_RATIO) lastSharpAtMs = now

        val source = PlanarYUVLuminanceSource(yuvBytes, width, height, 0, 0, width, height, false)
        val bitmap = BinaryBitmap(HybridBinarizer(source))
        val reader = MultiFormatReader().apply {
            setHints(
                mapOf(
                    DecodeHintType.POSSIBLE_FORMATS to listOf(BarcodeFormat.DATA_MATRIX),
                    DecodeHintType.TRY_HARDER to true,
                ),
            )
        }

        try {
            val result = reader.decode(bitmap)
            val decoded = DataMatrixCodec.resultToBytes(result)
            // Every real payload this app ever generates is well over this: the
            // shortest possible NavCode is its ~30-byte package-qualified prefix
            // alone (NavCode.GROUP_TAG) plus a category, a RunCode is bigger
            // still (base64 JSON), and a real shmevent.Msg (the join ticket
            // path) is bigger still. A single-digit/low-double-digit byte count
            // is ZXing's checksum occasionally passing on a spurious pattern
            // (camera noise, screen glare/moire) rather than an actual scanned
            // code -- observed live running the automated optical-scan harness
            // (pkg/e2erun/android_optical.go): repeated distinct "8 bytes"
            // decodes with no real code anywhere in frame, each one reaching
            // ScannerCoordinator.onScanned and re-triggering a confirmation
            // dialog, which under rapid repeats kept Compose's recomposer
            // perpetually busy and made the *real* scan moments later look, to
            // Espresso's idling check, like it never settled.
            // Silently dropping these here (never reaching onResult/
            // ScannerCoordinator at all) also spares a real human the same
            // spurious "couldn't decode this" dialog on an occasional stray frame.
            if (decoded.size < MIN_PLAUSIBLE_PAYLOAD_BYTES) {
                Log.i("KVDemo", "AUTO: discarding implausibly short DataMatrix decode (${decoded.size} bytes) as camera noise")
            } else {
                if (lastLoggedDecode?.contentEquals(decoded) != true) {
                    lastLoggedDecode = decoded
                    Log.i("KVDemo", "AUTO: DataMatrix decoded from camera frame (${decoded.size} bytes)")
                }
                onResult(decoded)
            }
        } catch (_: NotFoundException) {
            // No code found in this frame, ignore.
        }
    } catch (e: Exception) {
        Log.e("KVDemo", "Decode failed", e)
    } finally {
        imageProxy.close()
    }
}

/**
 * Home for the scanner's persisted torch/zoom settings -- SharedPreferences
 * rather than anything heavier, since it's exactly two small primitives
 * keyed by nothing else (there's only ever one camera). Re-applied once
 * per camera bind by [restoreSavedSettings] -- see the zoom observer in
 * [MainScannerWidget] for why that has to wait for the first real
 * min/max zoom report.
 */
object MainScannerManager {
    private const val PREFS_NAME = "scanner_settings"
    private const val KEY_TORCH_ON = "torch_on"
    private const val KEY_ZOOM_RATIO = "zoom_ratio"

    private var cameraControl: CameraControl? = null
    private var prefs: SharedPreferences? = null
    private val torchOn = mutableStateOf(false)

    fun setup(appContext: Context, control: CameraControl) {
        cameraControl = control
        if (prefs == null) {
            prefs = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        }
    }

    fun restoreSavedSettings(minZoom: Float, maxZoom: Float) {
        val savedZoom = prefs?.getFloat(KEY_ZOOM_RATIO, 1f) ?: 1f
        setZoom(savedZoom.coerceIn(minZoom, maxZoom), persist = false)

        if (prefs?.getBoolean(KEY_TORCH_ON, false) == true) {
            cameraControl?.enableTorch(true)
            torchOn.value = true
        }
    }

    fun toggleTorch() {
        cameraControl?.let {
            val newState = !torchOn.value
            it.enableTorch(newState)
            torchOn.value = newState
            prefs?.edit()?.putBoolean(KEY_TORCH_ON, newState)?.apply()
        }
    }

    /**
     * Asks [MainScannerWidget] to rebind its camera at the next watchdog tick (within
     * REFOCUS_INTERVAL_MS). The widget already rebinds itself when the analysis stream stops
     * producing new imagery, but that check cannot see the case where frames keep flowing and are
     * simply unusable -- confirmed on the real two-device rig by dumping the analyzer's own frames
     * mid-run: sharpness collapsed ~6x and stayed there, so every frame arrived fresh and none was
     * decodable, and no amount of refocusing recovered it. A caller that knows a code is definitely
     * in front of the camera and still isn't decoding has exactly the information this widget
     * lacks, and a fresh bind is what resets the camera's 3A state.
     */
    fun requestRebind() {
        rebindRequested = true
    }

    fun setZoom(zoom: Float, persist: Boolean = true) {
        cameraControl?.setZoomRatio(zoom)
        if (persist) {
            prefs?.edit()?.putFloat(KEY_ZOOM_RATIO, zoom)?.apply()
        }
    }
}
