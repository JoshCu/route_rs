pub mod fortran_modernized {
    //     type, bind(c) :: muskingum_cunge_input_t
    //         real(real32) :: dt      ! routing period in  seconds
    //         real(real32) :: qup     ! flow upstream previous timestep
    //         real(real32) :: quc     ! flow upstream current timestep
    //         real(real32) :: qdp     ! flow downstream previous timestep
    //         real(real32) :: ql      ! lateral inflow through reach (m^3/sec)
    //         real(real32) :: dx      ! channel lngth (m)
    //         real(real32) :: bw      ! bottom width (meters)
    //         real(real32) :: tw      ! top width before bankfull (meters)
    //         real(real32) :: tw_cc   ! top width of Compund (meters)
    //         real(real32) :: n       ! mannings coefficient
    //         real(real32) :: n_cc    ! mannings of compund
    //         real(real32) :: cs      ! Channel side slope slope
    //         real(real32) :: s0      ! Channel bottom slope %
    //         real(real32) :: depthp  ! depth of flow in channel
    //     end type

    use crate::kernel::muskingum::{MuskingumCungeInput, MuskingumCungeResult};

    unsafe extern "C" {
        pub fn c_binding_muskingum_cunge_t_route(
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
            c_binding_muskingum_cunge_t_route(&input, &mut result, calculate_courant);
        }
        result
    }
}

pub mod fortran_legacy {
    use crate::kernel::muskingum::{MuskingumCungeInput, MuskingumCungeResult};

    //     type, bind(c) :: muskingum_cunge_result_t
    //         real(real32) :: qdc     ! flow downstream current timestep
    //         real(real32) :: velc    ! channel velocity
    //         real(real32) :: depthc  ! depth of flow in channel
    //         real(real32) :: ck
    //         real(real32) :: cn
    //         real(real32) :: X
    //     end type

    unsafe extern "C" {
        pub fn c_binding_muskingum_cunge_t_route_legacy(
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
            depthp: depth_p,
            velp: 0.0,
        };
        let mut result = MuskingumCungeResult::default();
        unsafe {
            c_binding_muskingum_cunge_t_route_legacy(&input, &mut result, calculate_courant);
        }
        result
    }
}
