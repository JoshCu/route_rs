use crate::kernel::muskingum::MuskingumCungeResult;

/*

Direct port of Fortran kernel

Author: Brodie Alexander <baalexander2 [at] crimson.ua.edu>
Last Updated - 30 March 2026 by Brodie Alexander

*/
// macro_rules! call_kernel {
//     ($fn:path, $i:expr, $courant:expr) => {
//         $fn(
//             $i.qup, $i.quc, $i.qdp, $i.ql, $i.dt, $i.s0, $i.dx, $i.n, $i.cs, $i.bw, $i.tw,
//             $i.tw_cc, $i.n_cc, $i.depthp, $courant,
//         )
//     };
// }

pub fn muskingum_cunge(
    qup: f32,
    quc: f32,
    qdp: f32,
    ql: f32,
    dt: f32,
    s0: f32,
    dx: f32,
    n: f32,
    cs: f32,
    bw: f32,
    tw: f32,
    twcc: f32,

    ncc: f32,

    depthp: f32,

    _courant: bool,
) -> MuskingumCungeResult {
    let (mut qdc, mut velc, mut ck, mut cn, mut x) = (0.0, 0.0, 0.0, 0.0, 0.0);
    let mut h_1 = 0.0;
    let mut Qj = 0.0;
    let mut Qj_0 = 0.0;

    let mut maxiter = 100;

    let mindepth = 0.01_f32;

    let mut aerror = 0.01_f32;

    let mut rerror = 1.0_f32;

    let mut tries = 0;

    let (mut C1, mut C2, mut C3, mut C4) = (0.0, 0.0, 0.0, 0.0);

    let z = if cs == 0.0 { 1.0_f32 } else { 1.0_f32 / cs };

    let bfd = if bw > tw {
        bw / 0.00001
    } else if bw == tw {
        bw / (2.0 * z)
    } else {
        (tw - bw) / (2.0 * z)
    };

    let mut depthc = depthp.max(0.0);
    let mut h = (depthc * 1.33) + mindepth;
    let mut h_0 = depthc * 0.67;

    if ql > 0.0 || qup > 0.0 || quc > 0.0 || qdp > 0.0 || qdc > 0.0 {
        'l110: loop {
            let mut iter = 0;

            'do_while_large_error: while rerror > 0.01 && aerror >= mindepth && iter <= maxiter {
                let sec = secant2_h(
                    z, bw, bfd, twcc, s0, n, ncc, dt, dx, qdp, ql, qup, quc, h_0, 1, &mut Qj_0,
                    &mut C1, &mut C2, &mut C3, &mut C4, &mut x,
                );
                // Qj_0 = sec.Qj;

                let sec = secant2_h(
                    z, bw, bfd, twcc, s0, n, ncc, dt, dx, qdp, ql, qup, quc, h, 2, &mut Qj,
                    &mut C1, &mut C2, &mut C3, &mut C4, &mut x,
                );
                // x = sec.x;

                // Qj = sec.Qj;
                // (C1, C2, C3, C4) = (sec.C1, sec.C2, sec.C3, sec.C4);

                if (Qj_0 - Qj) != 0.0 {
                    h_1 = h - ((Qj * (h_0 - h)) / (Qj_0 - Qj));
                    if h_1 < 0.0 {
                        h_1 = h;
                    }
                } else {
                    h_1 = h;
                }

                if h > 0.0 {
                    rerror = f32::abs((h_1 - h) / h);
                    aerror = f32::abs(h_1 - h);
                } else {
                    rerror = 0.0;
                    aerror = 0.9;
                }

                h_0 = h.max(0.0);
                h = h_1.max(0.0);
                iter += 1;
                if h < mindepth {
                    break 'do_while_large_error; // goto 111;
                }
            }
            // 111    continue

            if iter >= maxiter {
                tries += 1;
                if tries <= 4 {
                    h *= 1.33;
                    h_0 *= 0.67;
                    maxiter += 25;
                    continue 'l110;
                }
            }

            if ((C1 * qup) + (C2 * quc) + (C3 * qdp) + C4) < 0.0 {
                if (C4 < 0.0) && (C4.abs() > (C1 * qup) + (C2 * quc) + (C3 * qdp)) {
                    qdc = 0.0;
                } else {
                    qdc = f32::max((C1 * qup) + (C2 * quc) + C4, (C1 * qup) + (C3 * qdp) + C4);
                }
            } else {
                qdc = (C1 * qup) + (C2 * quc) + (C3 * qdp) + C4;
            }

            let mut geom = hydraulic_geometry(h, bfd, bw, twcc, z);
            let twl = geom.twl;

            geom.r = (h * (bw + twl) / 2.0)
                / (bw + 2.0 * (((twl - bw) / 2.0).powf(2.0) + h.powf(2.0)).powf(0.5));
            velc = (1.0 / n) * (geom.r.powf(2.0 / 3.0)) * f32::sqrt(s0);
            depthc = h;
            break 'l110; // INVERSION OF FORTRAN GOTO: WE BREAK IF WE REACH THIS POINT
        }
    } else {
        qdc = 0.0;
        cn = 0.0;
        ck = 0.0;
        velc = 0.0;
        depthc = 0.0;
    }

    courant(h, bfd, bw, twcc, ncc, s0, n, z, dx, dt, &mut ck, &mut cn);

    MuskingumCungeResult {
        qdc,
        velc,
        depthc,
        ck,
        cn,
        x,
    }
}

// !**---------------------------------------------------**!
// !*                                                     *!
// !*                 SECANT2 SUBROUTINE                  *!
// !*                                                     *!
// !**---------------------------------------------------**!
#[allow(non_snake_case)]
#[derive(Default)]
struct Secant2H {
    Qj: f32,
    C1: f32,
    C2: f32,
    C3: f32,
    C4: f32,
    x: f32,
}
fn secant2_h(
    z: f32,
    bw: f32,
    bfd: f32,
    twcc: f32,
    s0: f32,
    n: f32,
    ncc: f32,
    dt: f32,
    dx: f32,
    qdp: f32,
    ql: f32,
    qup: f32,
    quc: f32,
    h: f32,
    interval: i32,
    Qj: &mut f32,
    C1: &mut f32,
    C2: &mut f32,
    C3: &mut f32,
    C4: &mut f32,
    X: &mut f32,
) -> Secant2H {
    let mut sec = Secant2H {
        Qj: *Qj,
        C1: *C1,
        C2: *C2,
        C3: *C3,
        C4: *C4,
        x: *X,
    };

    let (mut twl, mut wl) = (0.0, 0.0);

    let (mut wpc, mut area, mut areac) = (0.0, 0.0, 0.0);

    let (mut r, mut ck, mut cn, mut km, mut d) = (0.0, 0.0, 0.0, 0.0, 0.0);

    let (mut upper_interval, mut lower_interval) = (1, 2);

    let geom = hydraulic_geometry(h, bfd, bw, twcc, z);
    let (twl, r, area, areac, wp, wpc) =
        (geom.twl, geom.r, geom.area, geom.areac, geom.wp, geom.wpc);

    if h > bfd && twcc > 0.0 && ncc > 0.0 {
        ck = f32::max(
            0.0,
            ((f32::sqrt(s0) / n)
                * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                    - ((2.0 / 3.0)
                        * r.powf(5.0 / 3.0)
                        * (2.0 * f32::sqrt(1.0 + z * z) / (bw + 2.0 * bfd * z))))
                * area
                + ((f32::sqrt(s0) / (ncc)) * (5.0 / 3.0) * (h - bfd).powf(2.0 / 3.0)) * areac)
                / (area + areac),
        )
    } else if (h > 0.0) {
        ck = f32::max(
            0.0,
            (f32::sqrt(s0) / n)
                * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                    - ((2.0 / 3.0)
                        * r.powf(5.0 / 3.0)
                        * (2.0 * f32::sqrt(1.0 + z * z) / (bw + 2.0 * h * z)))),
        )
    } else {
        ck = 0.0;
    }

    if (ck > 0.0) {
        km = dt.max(dx / ck);
    } else {
        km = dt;
    }

    if h > bfd && twcc > 0.0 && ncc > 0.0 && ck > 0.0 {
        if (interval == upper_interval) {
            sec.x = (0.5 * (1.0 - (sec.Qj / (2.0 * twcc * s0 * ck * dx)))).clamp(0.0, 0.5)
        }

        if (interval == lower_interval) {
            sec.x = (0.5
                * (1.0
                    - (((sec.C1 * qup) + (sec.C2 * quc) + (sec.C3 * qdp) + sec.C4)
                        / (2.0 * twcc * s0 * ck * dx))))
                .clamp(0.25, 0.5)
        }
    } else if ck > 0.0 {
        if interval == upper_interval {
            sec.x = (0.5 * (1.0 - (sec.Qj / (2.0 * twl * s0 * ck * dx)))).clamp(0.0, 0.5)
        }
        if interval == lower_interval {
            sec.x = (0.5
                * (1.0
                    - (((sec.C1 * qup) + (sec.C2 * quc) + (sec.C3 * qdp) + sec.C4)
                        / (2.0 * twl * s0 * ck * dx))))
                .clamp(0.25, 0.5)
        }
    } else {
        sec.x = 0.0;
    }

    d = km * (1.0 - sec.x) + dt / 2.0;

    sec.C1 = (km * sec.x + dt / 2.0) / d;
    sec.C2 = (dt / 2.0 - km * sec.x) / d;
    sec.C3 = (km * (1.0 - sec.x) - dt / 2.0) / d;
    sec.C4 = (ql * dt) / d;

    if interval == lower_interval
        && sec.C4 < 0.0
        && sec.C4.abs() > (sec.C1 * qup + sec.C2 * quc + sec.C3 * qdp)
    {
        sec.C4 = -(sec.C1 * qup + sec.C2 * quc + sec.C3 * qdp);
    }

    sec.Qj = if (wp + wpc) > 0.0 {
        ((sec.C1 * qup) + (sec.C2 * quc) + (sec.C3 * qdp) + sec.C4)
            - ((1.0 / (((wp * n) + (wpc * ncc)) / (wp + wpc)))
                * (area + areac)
                * (r.powf(2.0 / 3.0))
                * f32::sqrt(s0))
    } else {
        0.0
    };
    *Qj = sec.Qj;
    *C1 = sec.C1;
    *C2 = sec.C2;
    *C3 = sec.C3;
    *C4 = sec.C4;
    *X = sec.x;
    sec
}

// !**---------------------------------------------------**!
// !*                                                     *!
// !*                 COURANT SUBROUTINE                  *!
// !*                                                     *!
// !**---------------------------------------------------**!
fn courant(
    h: f32,
    bfd: f32,
    bw: f32,
    twcc: f32,
    ncc: f32,
    s0: f32,
    n: f32,
    z: f32,
    dx: f32,
    dt: f32,
    ck: &mut f32,
    cn: &mut f32,
) {
    let geom = hydraulic_geometry(h, bfd, bw, twcc, z);

    *ck = f32::max(
        0.0,
        ((f32::sqrt(s0) / n)
            * ((5.0 / 3.0) * geom.r.powf(2.0 / 3.0)
                - ((2.0 / 3.0)
                    * geom.r.powf(5.0 / 3.0)
                    * (2.0 * f32::sqrt(1.0 + z * z) / (bw + 2.0 * geom.h_lt_bf * z))))
            * geom.area
            + ((f32::sqrt(s0) / (ncc)) * (5.0 / 3.0) * (geom.h_gt_bf).powf(2.0 / 3.0))
                * geom.areac)
            / (geom.area + geom.areac),
    );

    *cn = *ck * (dt / dx);
}

// !**---------------------------------------------------**!
// !*                                                     *!
// !*           Hydraulic Geometry SUBROUTINE             *!
// !*                                                     *!
// !**---------------------------------------------------**!

#[derive(Default)]
struct HGeometry {
    twl: f32,
    r: f32,
    area: f32,
    areac: f32,
    wp: f32,
    wpc: f32,
    h_gt_bf: f32,
    h_lt_bf: f32,
}
fn hydraulic_geometry(h: f32, bfd: f32, bw: f32, twcc: f32, z: f32) -> HGeometry {
    let mut geom = HGeometry {
        h_gt_bf: 0.0_f32.max(h - bfd),
        h_lt_bf: h.min(bfd),
        twl: bw + 2.0 * z * h,
        ..Default::default()
    };

    if (geom.h_gt_bf > 0.0) && (twcc <= 0.0) {
        geom.h_gt_bf = 0.0;
        geom.h_lt_bf = h;
    }

    geom.area = (bw + geom.h_lt_bf * z) * geom.h_lt_bf;

    geom.wp = bw + 2.0 * geom.h_lt_bf * f32::sqrt(1.0 + z * z);

    geom.areac = twcc * geom.h_gt_bf;

    geom.wpc = if geom.h_gt_bf > 0.0 {
        twcc + (2.0 * geom.h_gt_bf)
    } else {
        0.0
    };

    geom.r = (geom.area + geom.areac) / (geom.wp + geom.wpc);

    geom
}
