use super::{
    OutboundCropFocus, OutboundImageFormat, OutboundImageOptimizerOptions,
    OutboundImageOptimizerRegion, OutboundResizeFilter,
};
use crate::error::EdgeError;

impl OutboundImageOptimizerOptions {
    /// Selects a nonzero crop ratio and validates both percentage coordinates, if present.
    ///
    /// # Errors
    /// Returns a client error for zero ratio dimensions or a coordinate above 100.
    #[inline]
    pub fn crop(
        mut self,
        width: u32,
        height: u32,
        focus: OutboundCropFocus,
    ) -> Result<Self, EdgeError> {
        if width == 0 || height == 0 {
            return Err(EdgeError::bad_request(
                "image optimizer crop dimensions must be nonzero",
            ));
        }
        if matches!(focus, OutboundCropFocus::Percent { x, y } if x > 100 || y > 100) {
            return Err(EdgeError::bad_request(
                "image optimizer crop percentages must be between 0 and 100",
            ));
        }
        self.crop = Some((width, height, focus));
        Ok(self)
    }

    #[must_use]
    #[inline]
    pub fn crop_setting(&self) -> Option<(u32, u32, OutboundCropFocus)> {
        self.crop
    }

    #[must_use]
    #[inline]
    pub fn format(mut self, format: OutboundImageFormat) -> Self {
        self.format = Some(format);
        self
    }

    #[must_use]
    #[inline]
    pub fn format_setting(&self) -> Option<OutboundImageFormat> {
        self.format
    }

    /// Selects a nonzero output height.
    ///
    /// # Errors
    /// Returns a client error for zero.
    #[inline]
    pub fn height(mut self, height: u32) -> Result<Self, EdgeError> {
        if height == 0 {
            return Err(EdgeError::bad_request(
                "image optimizer height must be nonzero",
            ));
        }
        self.height = Some(height);
        Ok(self)
    }

    #[must_use]
    #[inline]
    pub fn height_setting(&self) -> Option<u32> {
        self.height
    }

    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    #[inline]
    pub fn preserve_origin_query(mut self, preserve: bool) -> Self {
        self.preserve_origin_query = preserve;
        self
    }

    #[must_use]
    #[inline]
    pub fn preserve_origin_query_setting(&self) -> bool {
        self.preserve_origin_query
    }

    /// Selects quality in the inclusive 0..=100 range.
    ///
    /// # Errors
    /// Returns a client error for values above 100.
    #[inline]
    pub fn quality(mut self, quality: u8) -> Result<Self, EdgeError> {
        if quality > 100 {
            return Err(EdgeError::bad_request(
                "image optimizer quality must be between 0 and 100",
            ));
        }
        self.quality = Some(quality);
        Ok(self)
    }

    #[must_use]
    #[inline]
    pub fn quality_setting(&self) -> Option<u8> {
        self.quality
    }

    #[must_use]
    #[inline]
    pub fn region(mut self, region: OutboundImageOptimizerRegion) -> Self {
        self.region = Some(region);
        self
    }

    #[must_use]
    #[inline]
    pub fn region_setting(&self) -> Option<&OutboundImageOptimizerRegion> {
        self.region.as_ref()
    }

    #[must_use]
    #[inline]
    pub fn resize_filter(mut self, filter: OutboundResizeFilter) -> Self {
        self.resize_filter = Some(filter);
        self
    }

    #[must_use]
    #[inline]
    pub fn resize_filter_setting(&self) -> Option<OutboundResizeFilter> {
        self.resize_filter
    }

    /// Selects a nonzero output width.
    ///
    /// # Errors
    /// Returns a client error for zero.
    #[inline]
    pub fn width(mut self, width: u32) -> Result<Self, EdgeError> {
        if width == 0 {
            return Err(EdgeError::bad_request(
                "image optimizer width must be nonzero",
            ));
        }
        self.width = Some(width);
        Ok(self)
    }

    #[must_use]
    #[inline]
    pub fn width_setting(&self) -> Option<u32> {
        self.width
    }
}

impl OutboundImageOptimizerRegion {
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        self.token
    }

    /// Accepts exactly the seven region tokens serialized by Fastly SDK 0.13.1.
    ///
    /// # Errors
    /// Returns a fixed client error for any other token, without echoing input.
    #[inline]
    pub fn parse(token: &str) -> Result<Self, EdgeError> {
        let region = match token {
            "asia" => "asia",
            "australia" => "australia",
            "eu_central" => "eu_central",
            "eu_west" => "eu_west",
            "us_central" => "us_central",
            "us_east" => "us_east",
            "us_west" => "us_west",
            _ => return Err(EdgeError::bad_request("invalid image optimizer region")),
        };
        Ok(Self { token: region })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        EdgeError, OutboundCropFocus, OutboundImageFormat, OutboundImageOptimizerOptions,
        OutboundImageOptimizerRegion, OutboundResizeFilter,
    };

    #[test]
    fn image_optimizer_defaults_leave_every_selector_unset() {
        let options = OutboundImageOptimizerOptions::new();
        assert_eq!(options, OutboundImageOptimizerOptions::default());
        assert!(options.region_setting().is_none());
        assert!(!options.preserve_origin_query_setting());
        assert!(options.format_setting().is_none());
        assert!(options.quality_setting().is_none());
        assert!(options.resize_filter_setting().is_none());
        assert!(options.width_setting().is_none());
        assert!(options.height_setting().is_none());
        assert!(options.crop_setting().is_none());
    }

    #[test]
    fn image_optimizer_regions_match_the_pinned_sdk_tokens() {
        for token in [
            "us_east",
            "us_central",
            "us_west",
            "eu_central",
            "eu_west",
            "asia",
            "australia",
        ] {
            let region = OutboundImageOptimizerRegion::parse(token).expect("SDK region");
            assert_eq!(region.as_str(), token);
            let options = OutboundImageOptimizerOptions::new().region(region.clone());
            assert_eq!(options.region_setting(), Some(&region));
        }
    }

    #[test]
    fn image_optimizer_region_rejects_aliases_and_untrusted_tokens() {
        for token in [
            "",
            "US_EAST",
            "us-east",
            " us_east",
            "us_east ",
            "us_east\n",
            "us_east\0",
            "europe",
            "global",
            "asia/token-secret",
            "\u{e9}",
        ] {
            let error = OutboundImageOptimizerRegion::parse(token).expect_err("invalid region");
            assert!(matches!(error, EdgeError::BadRequest { .. }));
            assert!(!error.to_string().contains("token-secret"));
        }
    }

    #[test]
    fn image_optimizer_quality_accepts_exactly_zero_through_one_hundred() {
        for quality in 0_u8..=u8::MAX {
            let result = OutboundImageOptimizerOptions::new().quality(quality);
            if quality <= 100 {
                assert_eq!(
                    result.expect("valid quality").quality_setting(),
                    Some(quality)
                );
            } else {
                assert!(matches!(result, Err(EdgeError::BadRequest { .. })));
            }
        }
    }

    #[test]
    fn image_optimizer_dimensions_and_crop_reject_zero_without_invented_maxima() {
        for dimension in [0, 1, u32::MAX] {
            let width = OutboundImageOptimizerOptions::new().width(dimension);
            let height = OutboundImageOptimizerOptions::new().height(dimension);
            if dimension == 0 {
                assert!(matches!(width, Err(EdgeError::BadRequest { .. })));
                assert!(matches!(height, Err(EdgeError::BadRequest { .. })));
            } else {
                assert_eq!(
                    width.expect("positive width").width_setting(),
                    Some(dimension)
                );
                assert_eq!(
                    height.expect("positive height").height_setting(),
                    Some(dimension)
                );
            }
        }
        for (width, height) in [(0, 0), (0, 1), (1, 0), (1, 1), (u32::MAX, u32::MAX)] {
            let result = OutboundImageOptimizerOptions::new().crop(
                width,
                height,
                OutboundCropFocus::Default,
            );
            if width == 0 || height == 0 {
                assert!(matches!(result, Err(EdgeError::BadRequest { .. })));
            } else {
                assert_eq!(
                    result.expect("positive ratio").crop_setting(),
                    Some((width, height, OutboundCropFocus::Default))
                );
            }
        }
    }

    #[test]
    fn image_optimizer_crop_checks_both_public_percent_payloads() {
        for (x, y) in [
            (0, 0),
            (0, 100),
            (100, 0),
            (100, 100),
            (101, 0),
            (0, 101),
            (255, 255),
        ] {
            let focus = OutboundCropFocus::Percent { x, y };
            let result = OutboundImageOptimizerOptions::new().crop(16, 9, focus);
            if x <= 100 && y <= 100 {
                assert_eq!(
                    result.expect("percent focus").crop_setting(),
                    Some((16, 9, focus))
                );
            } else {
                assert!(matches!(result, Err(EdgeError::BadRequest { .. })));
            }
        }
        for focus in [OutboundCropFocus::Default, OutboundCropFocus::Smart] {
            assert_eq!(
                OutboundImageOptimizerOptions::new()
                    .crop(16, 9, focus)
                    .expect("focus")
                    .crop_setting(),
                Some((16, 9, focus))
            );
        }
    }

    #[test]
    fn image_optimizer_readbacks_preserve_independent_settings_and_replacement() {
        let region = OutboundImageOptimizerRegion::parse("eu_west").expect("region");
        let options = OutboundImageOptimizerOptions::new()
            .region(region.clone())
            .preserve_origin_query(true)
            .format(OutboundImageFormat::WebP)
            .quality(0)
            .expect("quality")
            .quality(100)
            .expect("quality replacement")
            .resize_filter(OutboundResizeFilter::Lanczos3)
            .width(320)
            .expect("width")
            .height(180)
            .expect("height")
            .crop(16, 9, OutboundCropFocus::Percent { x: 20, y: 80 })
            .expect("crop");
        assert_eq!(options.region_setting(), Some(&region));
        assert!(options.preserve_origin_query_setting());
        assert_eq!(options.format_setting(), Some(OutboundImageFormat::WebP));
        assert_eq!(options.quality_setting(), Some(100));
        assert_eq!(
            options.resize_filter_setting(),
            Some(OutboundResizeFilter::Lanczos3)
        );
        assert_eq!(options.width_setting(), Some(320));
        assert_eq!(options.height_setting(), Some(180));
        assert_eq!(
            options.crop_setting(),
            Some((16, 9, OutboundCropFocus::Percent { x: 20, y: 80 }))
        );
        let changed = options
            .clone()
            .preserve_origin_query(false)
            .format(OutboundImageFormat::Auto)
            .resize_filter(OutboundResizeFilter::Nearest)
            .width(1)
            .expect("width replacement")
            .height(2)
            .expect("height replacement")
            .crop(1, 2, OutboundCropFocus::Smart)
            .expect("crop replacement")
            .region(OutboundImageOptimizerRegion::parse("asia").expect("replacement region"));
        assert_eq!(changed.region_setting().expect("region").as_str(), "asia");
        assert!(!changed.preserve_origin_query_setting());
        assert_eq!(changed.format_setting(), Some(OutboundImageFormat::Auto));
        assert_eq!(
            changed.resize_filter_setting(),
            Some(OutboundResizeFilter::Nearest)
        );
        assert_eq!(changed.width_setting(), Some(1));
        assert_eq!(changed.height_setting(), Some(2));
        assert_eq!(
            changed.crop_setting(),
            Some((1, 2, OutboundCropFocus::Smart))
        );
        assert_eq!(options.clone(), options);
        assert_ne!(changed, options);
    }

    #[test]
    fn image_optimizer_all_selectors_have_copy_equality_readback() {
        for format in [
            OutboundImageFormat::Auto,
            OutboundImageFormat::Avif,
            OutboundImageFormat::Gif,
            OutboundImageFormat::Jpeg,
            OutboundImageFormat::JpegXl,
            OutboundImageFormat::Mp4,
            OutboundImageFormat::Png,
            OutboundImageFormat::WebP,
        ] {
            assert_eq!(
                OutboundImageOptimizerOptions::new()
                    .format(format)
                    .format_setting(),
                Some(format)
            );
        }
        for filter in [
            OutboundResizeFilter::Nearest,
            OutboundResizeFilter::Bilinear,
            OutboundResizeFilter::Bicubic,
            OutboundResizeFilter::Lanczos2,
            OutboundResizeFilter::Lanczos3,
        ] {
            assert_eq!(
                OutboundImageOptimizerOptions::new()
                    .resize_filter(filter)
                    .resize_filter_setting(),
                Some(filter)
            );
        }
    }
}
