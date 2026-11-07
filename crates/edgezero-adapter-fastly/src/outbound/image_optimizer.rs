use edgezero_core::{
    EdgeError, OutboundCropFocus, OutboundImageFormat, OutboundImageOptimizerOptions,
    OutboundImageOptimizerRegion, OutboundResizeFilter,
};
use fastly::image_optimizer::{
    Area, Crop, CropMode, Format, ImageOptimizerOptions, ImageOptimizerRegion, PixelsOrPercentage,
    PointOrOffset, Position, ResizeAlgorithm,
};

pub(super) fn native_options(
    options: &OutboundImageOptimizerOptions,
) -> Result<ImageOptimizerOptions, EdgeError> {
    let region = match options.region_setting() {
        Some(OutboundImageOptimizerRegion::Asia) => ImageOptimizerRegion::Asia,
        Some(OutboundImageOptimizerRegion::Australia) => ImageOptimizerRegion::Australia,
        Some(OutboundImageOptimizerRegion::EuCentral) => ImageOptimizerRegion::EuCentral,
        Some(OutboundImageOptimizerRegion::EuWest) => ImageOptimizerRegion::EuWest,
        Some(OutboundImageOptimizerRegion::UsCentral) => ImageOptimizerRegion::UsCentral,
        Some(OutboundImageOptimizerRegion::UsEast) => ImageOptimizerRegion::UsEast,
        Some(OutboundImageOptimizerRegion::UsWest) => ImageOptimizerRegion::UsWest,
        None => {
            return Err(EdgeError::bad_request(
                "image_optimizer requires an explicit region",
            ));
        }
    };
    let mut native = ImageOptimizerOptions::from_region(region);
    native.preserve_query_string_on_origin_request = Some(options.preserve_origin_query_setting());
    native.format = options.format_setting().map(|format| match format {
        OutboundImageFormat::Auto => Format::Auto,
        OutboundImageFormat::Avif => Format::AVIF,
        OutboundImageFormat::Gif => Format::GIF,
        OutboundImageFormat::Jpeg => Format::JPEG,
        OutboundImageFormat::JpegXl => Format::JPEGXL,
        OutboundImageFormat::Mp4 => Format::MP4,
        OutboundImageFormat::Png => Format::PNG,
        OutboundImageFormat::WebP => Format::WebP,
    });
    native.quality = options.quality_setting().map(u32::from);
    native.width = options.width_setting().map(PixelsOrPercentage::Pixels);
    native.height = options.height_setting().map(PixelsOrPercentage::Pixels);
    native.resize_filter = options.resize_filter_setting().map(|filter| match filter {
        OutboundResizeFilter::Bicubic => ResizeAlgorithm::Bicubic,
        OutboundResizeFilter::Bilinear => ResizeAlgorithm::Bilinear,
        OutboundResizeFilter::Lanczos2 => ResizeAlgorithm::Lanczos2,
        OutboundResizeFilter::Lanczos3 => ResizeAlgorithm::Lanczos3,
        OutboundResizeFilter::Nearest => ResizeAlgorithm::Nearest,
    });
    native.crop = options.crop_setting().map(|(width, height, focus)| {
        let (position, mode) = match focus {
            OutboundCropFocus::Default => (None, None),
            OutboundCropFocus::Smart => (None, Some(CropMode::Smart)),
            OutboundCropFocus::Percent { x, y } => (
                Some(Position {
                    x: Some(PointOrOffset::Offset(u32::from(x))),
                    y: Some(PointOrOffset::Offset(u32::from(y))),
                }),
                None,
            ),
        };
        Crop {
            size: Area::AspectRatio((width, height)),
            position,
            mode,
        }
    });
    Ok(native)
}

#[cfg(test)]
mod tests {
    use edgezero_core::{
        OutboundCropFocus, OutboundImageFormat, OutboundImageOptimizerOptions,
        OutboundImageOptimizerRegion, OutboundResizeFilter,
    };
    use fastly::image_optimizer::{
        Area, Crop, CropMode, Format, ImageOptimizerOptions, ImageOptimizerRegion,
        PixelsOrPercentage, PointOrOffset, Position, ResizeAlgorithm,
    };

    use super::native_options;

    #[test]
    fn image_optimizer_native_mapping_matches_sdk() {
        for (region, native_region) in [
            (
                OutboundImageOptimizerRegion::Asia,
                ImageOptimizerRegion::Asia,
            ),
            (
                OutboundImageOptimizerRegion::Australia,
                ImageOptimizerRegion::Australia,
            ),
            (
                OutboundImageOptimizerRegion::EuCentral,
                ImageOptimizerRegion::EuCentral,
            ),
            (
                OutboundImageOptimizerRegion::EuWest,
                ImageOptimizerRegion::EuWest,
            ),
            (
                OutboundImageOptimizerRegion::UsCentral,
                ImageOptimizerRegion::UsCentral,
            ),
            (
                OutboundImageOptimizerRegion::UsEast,
                ImageOptimizerRegion::UsEast,
            ),
            (
                OutboundImageOptimizerRegion::UsWest,
                ImageOptimizerRegion::UsWest,
            ),
        ] {
            for preserve in [false, true] {
                let options = OutboundImageOptimizerOptions::new()
                    .region(region)
                    .preserve_origin_query(preserve);
                let mut expected = ImageOptimizerOptions::from_region(native_region.clone());
                expected.preserve_query_string_on_origin_request = Some(preserve);
                assert_eq!(
                    serde_json::to_value(native_options(&options).unwrap()).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
        }
        for (format, native_format) in [
            (OutboundImageFormat::Auto, Format::Auto),
            (OutboundImageFormat::Avif, Format::AVIF),
            (OutboundImageFormat::Gif, Format::GIF),
            (OutboundImageFormat::Jpeg, Format::JPEG),
            (OutboundImageFormat::JpegXl, Format::JPEGXL),
            (OutboundImageFormat::Mp4, Format::MP4),
            (OutboundImageFormat::Png, Format::PNG),
            (OutboundImageFormat::WebP, Format::WebP),
        ] {
            let options = OutboundImageOptimizerOptions::new()
                .region(OutboundImageOptimizerRegion::UsEast)
                .format(format);
            let actual = native_options(&options).unwrap();
            assert_eq!(
                serde_json::to_value(actual.format).unwrap(),
                serde_json::to_value(native_format).unwrap()
            );
            assert!(actual.auto.is_none());
        }
        for (filter, native_filter) in [
            (OutboundResizeFilter::Bicubic, ResizeAlgorithm::Bicubic),
            (OutboundResizeFilter::Bilinear, ResizeAlgorithm::Bilinear),
            (OutboundResizeFilter::Lanczos2, ResizeAlgorithm::Lanczos2),
            (OutboundResizeFilter::Lanczos3, ResizeAlgorithm::Lanczos3),
            (OutboundResizeFilter::Nearest, ResizeAlgorithm::Nearest),
        ] {
            let options = OutboundImageOptimizerOptions::new()
                .region(OutboundImageOptimizerRegion::Asia)
                .resize_filter(filter);
            let actual = native_options(&options).unwrap();
            assert_eq!(
                serde_json::to_value(actual.resize_filter).unwrap(),
                serde_json::to_value(native_filter).unwrap()
            );
        }
    }

    #[test]
    fn image_optimizer_native_crop_and_dimensions_match_sdk() {
        for (focus, position, mode) in [
            (OutboundCropFocus::Default, None, None),
            (OutboundCropFocus::Smart, None, Some(CropMode::Smart)),
            (
                OutboundCropFocus::Percent { x: 0, y: 100 },
                Some(Position {
                    x: Some(PointOrOffset::Offset(0)),
                    y: Some(PointOrOffset::Offset(100)),
                }),
                None,
            ),
        ] {
            for quality in [0, 100] {
                let options = OutboundImageOptimizerOptions::new()
                    .region(OutboundImageOptimizerRegion::UsWest)
                    .crop(16, 9, focus)
                    .unwrap()
                    .width(u32::MAX)
                    .unwrap()
                    .height(1)
                    .unwrap()
                    .quality(quality)
                    .unwrap();
                let actual = native_options(&options).unwrap();
                let crop = Crop {
                    size: Area::AspectRatio((16, 9)),
                    position: position.clone(),
                    mode: mode.clone(),
                };
                assert_eq!(
                    serde_json::to_value(actual.crop).unwrap(),
                    serde_json::to_value(crop).unwrap()
                );
                assert!(matches!(
                    actual.width,
                    Some(PixelsOrPercentage::Pixels(u32::MAX))
                ));
                assert!(matches!(actual.height, Some(PixelsOrPercentage::Pixels(1))));
                assert_eq!(actual.quality, Some(u32::from(quality)));
            }
        }
        native_options(&OutboundImageOptimizerOptions::new())
            .expect_err("explicit region required");
    }
}
