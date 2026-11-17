use worker::js_sys::{Function, Reflect, global};
use worker::wasm_bindgen::JsValue;

#[derive(Debug)]
pub(crate) struct NumericTimeoutGuard {
    original: JsValue,
}

impl NumericTimeoutGuard {
    pub(crate) fn install() -> Self {
        let scope = global();
        let key = JsValue::from_str("setTimeout");
        let original = Reflect::get(&scope, &key).expect("original timeout function");
        // Node timer handles coerce to numeric IDs accepted by both timer APIs.
        let factory = Function::new_with_args(
            "original",
            "return (...args) => Number(original.apply(globalThis, args));",
        );
        let numeric = factory
            .call1(&JsValue::UNDEFINED, &original)
            .expect("numeric timeout wrapper");
        assert!(Reflect::set(&scope, &key, &numeric).expect("install numeric timeout wrapper"));
        Self { original }
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for NumericTimeoutGuard {
    fn drop(&mut self) {
        drop(Reflect::set(
            &global(),
            &JsValue::from_str("setTimeout"),
            &self.original,
        ));
    }
}
