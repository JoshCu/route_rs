/// C implementation of Muskingum-Cunge routing.
///
/// Known issues that cause significant divergence from the Fortran/Rust kernels:
///
///   - Uses `double` precision internally while input/output are `float`, causing
///     different rounding behavior throughout the computation.
///
///   - Bug when `cs == 0`: sets `z = 1.0` then immediately overwrites to `z = sqrt(2)`,
///     and `sqrt_1z2` is left uninitialized (muskingumcunge.c lines 194-196).
///
///   - Different secant method structure: uses separate left/right QHC structs with
///     different Xmin values (0.0 for left vs 0.25 for right).
///
///   - Different negative-flow handling: the else branch (line 302) always computes
///     `fmax` of two terms even when total flow is positive, whereas Fortran/Rust
///     return the flow sum directly.
///
///   - The C QVD_float struct has `cn, ck` field order while the Rust
///     MuskingumCungeResult has `ck, cn` — the ck/cn values are swapped in the
///     returned result (does not affect flow/velocity/depth).
use crate::kernel::muskingum::MuskingumCungeResult;

#[repr(C)]
#[derive(Default)]
pub struct MuskingumCungeInput {
    /// routing period in  seconds
    dt: f32,
    ///flow upstream previous timestep
    qup: f32,
    /// flow upstream current timestep
    quc: f32,
    /// flow downstream previous timestep
    qdp: f32,
    /// lateral inflow through reach (m^3/sec)
    ql: f32,
    /// channel lngth (m)
    dx: f32,
    /// bottom width (meters)
    bw: f32,
    /// top width before bankfull (meters)
    tw: f32,
    /// top width of Compund (meters)
    tw_cc: f32,
    /// mannings coefficient
    n: f32,
    /// mannings of compund
    n_cc: f32,
    /// Channel side slope slope
    cs: f32,
    /// Channel bottom slope %
    s0: f32,
    /// Velocity at previous timestep (currently unused)
    velp: f32,
    /// depth of flow in channel
    depthp: f32,
}

unsafe extern "C" {
    pub fn c_binding_c_mc_muskingum_cunge(
        input: *const MuskingumCungeInput,
        result: *mut MuskingumCungeResult,
        calculate_courant: bool,
    );
}

pub fn submuskingcunge(
    qup: f32,                 // flow upstream previous timestep
    quc: f32,                 // flow upstream current timestep
    qdp: f32,                 // flow downstream previous timestep
    ql: f32,                  // lateral inflow through reach (m^3/sec)
    dt: f32,                  // routing period in seconds
    so: f32,                  // channel bottom slope (as fraction, not %)
    dx: f32,                  // channel length (m)
    n: f32,                   // mannings coefficient
    cs: f32,                  // channel side slope
    bw: f32,                  // bottom width (meters)
    tw: f32,                  // top width before bankfull (meters)
    tw_cc: f32,               // top width of compound (meters)
    n_cc: f32,                // mannings of compound
    depth_p: f32,            // depth of flow in channel
    calculate_courant: bool, // whether to calculate courant number
) -> MuskingumCungeResult {
    // double dt,
    // double qup,
    // double quc,
    // double qdp,
    // double ql,
    // double dx,
    // double bw,
    // double tw,
    // double twcc,
    // double n,
    // double ncc,
    // double cs,
    // double s0,
    // double velp,
    // double depthp,
    let input = MuskingumCungeInput {
        dt,
        qup,
        quc,
        qdp,
        ql,
        dx,
        bw,
        tw,
        tw_cc,
        n,
        n_cc,
        cs,
        s0: so,
        velp: 0.0,
        depthp: depth_p,
    };
    let mut result = MuskingumCungeResult::default();
    unsafe {
        c_binding_c_mc_muskingum_cunge(&input, &mut result, calculate_courant);
    }
    result
}
