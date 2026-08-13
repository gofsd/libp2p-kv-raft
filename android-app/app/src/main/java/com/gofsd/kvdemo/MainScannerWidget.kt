package com.gofsd.kvdemo

import android.content.Context
import android.content.SharedPreferences
import android.hardware.camera2.CaptureRequest
import android.util.Log
import android.util.Size
import androidx.camera.camera2.interop.Camera2Interop
import androidx.camera.camera2.interop.ExperimentalCamera2Interop
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
    val previewView = remember { PreviewView(context) }

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
                    // reliably. 1280x960 is still comfortably fast to decode per frame on any
                    // device this app targets, but gives roughly 4x the pixels to work with.
                    Size(1280, 960),
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
                    processImageProxySafe(imageProxy, onResult)
                    analyzerScanning.set(false)
                }
            }

        cameraProvider.unbindAll()
        val camera = cameraProvider.bindToLifecycle(
            lifecycleOwner,
            CameraSelector.DEFAULT_BACK_CAMERA,
            preview,
            imageAnalyzer,
        )
        Log.i("KVDemo", "AUTO: camera bound, scanner live")

        MainScannerManager.setup(context.applicationContext, camera.cameraControl)

        // The saved zoom can only be re-applied once the camera has reported its real
        // min/max ratio -- restoring before that would clamp against the placeholder 1f..1f
        // range every bind. The first zoom report is that signal, hence the one-shot flag.
        var settingsRestored = false

        camera.cameraInfo.zoomState.observe(lifecycleOwner) { state ->
            zoomRatio = state.zoomRatio
            minZoom = state.minZoomRatio
            maxZoom = state.maxZoomRatio
            if (!settingsRestored) {
                settingsRestored = true
                MainScannerManager.restoreSavedSettings(minZoom, maxZoom)
            }
        }

        camera.cameraInfo.torchState.observe(lifecycleOwner) { state ->
            torchOn = (state == TorchState.ON)
        }

        // See REFOCUS_INTERVAL_MS's own doc comment. Runs for as long as this LaunchedEffect does
        // (the widget's whole lifetime -- cancelled automatically on dispose, same as the analyzer
        // binding above it), centered on the frame since that's where ScanFrameOverlay's own
        // target window is drawn.
        while (isActive) {
            delay(REFOCUS_INTERVAL_MS)
            val point = previewView.meteringPointFactory.createPoint(0.5f, 0.5f)
            val action = FocusMeteringAction.Builder(point, FocusMeteringAction.FLAG_AF).disableAutoCancel().build()
            camera.cameraControl.startFocusAndMetering(action)
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
        val yuvBytes = ByteArray(yBuffer.remaining())
        yBuffer.get(yuvBytes)

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

    fun setZoom(zoom: Float, persist: Boolean = true) {
        cameraControl?.setZoomRatio(zoom)
        if (persist) {
            prefs?.edit()?.putFloat(KEY_ZOOM_RATIO, zoom)?.apply()
        }
    }
}
