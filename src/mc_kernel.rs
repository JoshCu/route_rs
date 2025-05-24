/// Muskingum-Cunge routing implementation for channel flow calculations
/// Updated to match Fortran version from NWM
pub fn submuskingcunge(
    qup: f64,     // flow upstream previous timestep
    quc: f64,     // flow upstream current timestep
    qdp: f64,     // flow downstream previous timestep
    ql: f64,      // lateral inflow through reach (m^3/sec)
    dt: f64,      // routing period in seconds
    so: f64,      // channel bottom slope (as fraction, not %)
    dx: f64,      // channel length (m)
    n: f64,       // mannings coefficient
    cs: f64,      // channel side slope
    bw: f64,      // bottom width (meters)
    tw: f64,      // top width before bankfull (meters)
    tw_cc: f64,   // top width of compound (meters)
    n_cc: f64,    // mannings of compound
    depth_p: f64, // depth of flow in channel
) -> (f64, f64, f64, f64, f64, f64) {
    // Returns (qdc, velc, depthc, ck, cn, x)

    // Local variables
    let mut c1: f64 = 0.0;
    let mut c2: f64 = 0.0;
    let mut c3: f64 = 0.0;
    let mut c4: f64 = 0.0;
    let mut km: f64;
    let mut x: f64 = 0.0;
    let mut ck: f64 = 0.0;
    let mut cn: f64 = 0.0;

    // Channel geometry and characteristics
    let mut twl: f64;
    let mut area: f64;
    let mut area_c: f64;
    let z: f64;
    let mut r: f64;
    let mut wp: f64;
    let mut wp_c: f64;
    let mut h: f64;
    let mut h_0: f64;
    let mut h_1: f64;
    let bfd: f64;
    let mut qj_0: f64 = 0.0;
    let mut qj: f64 = 0.0;
    let mut d: f64;
    let mut aerror: f64 = 0.01;
    let mut rerror: f64 = 1.0;
    let mut iter: i32;
    let mut maxiter: i32 = 100;
    let mindepth: f64 = 0.01;
    let mut tries: i32 = 0;

    // Set trapezoid distance
    z = if cs == 0.0 { 1.0 } else { 1.0 / cs };

    // Calculate bankfull depth
    bfd = if bw > tw {
        bw / 0.00001
    } else if bw == tw {
        bw / (2.0 * z)
    } else {
        (tw - bw) / (2.0 * z)
    };

    // Check for invalid channel coefficients
    if n <= 0.0 || so <= 0.0 || z <= 0.0 || bw <= 0.0 {
        panic!(
            "Error in channel coefficients -> Muskingum cunge: n={}, so={}, z={}, bw={}",
            n, so, z, bw
        );
    }

    // Initialize depth
    let mut depth_c = f64::max(depth_p, 0.0);
    h = (depth_c * 1.33) + mindepth;
    h_0 = depth_c * 0.67;

    let qdc: f64;
    let velc: f64;

    // Only solve if there's water to flux
    if ql > 0.0 || qup > 0.0 || quc > 0.0 || qdp > 0.0 {
        'outer: loop {
            iter = 0;

            // Secant method loop
            while rerror > 0.01 && aerror >= mindepth && iter <= maxiter {
                // Lower interval (h_0)
                wp_c = 0.0;
                area = 0.0;
                area_c = 0.0;

                // Calculate hydraulic geometry for h_0
                twl = bw + 2.0 * z * h_0;

                if h_0 > bfd && tw_cc > 0.0 && n_cc > 0.0 {
                    // Water outside of defined channel
                    area = (bw + bfd * z) * bfd;
                    area_c = tw_cc * (h_0 - bfd);
                    wp = bw + 2.0 * bfd * (1.0 + z * z).sqrt();
                    wp_c = tw_cc + 2.0 * (h_0 - bfd);
                    r = (area + area_c) / (wp + wp_c);
                } else {
                    area = (bw + h_0 * z) * h_0;
                    wp = bw + 2.0 * h_0 * (1.0 + z * z).sqrt();
                    r = if wp > 0.0 { area / wp } else { 0.0 };
                }

                // Calculate kinematic celerity
                if h_0 > bfd && tw_cc > 0.0 && n_cc > 0.0 {
                    ck = f64::max(
                        0.0,
                        ((so.sqrt() / n)
                            * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                - (2.0 / 3.0)
                                    * r.powf(5.0 / 3.0)
                                    * (2.0 * (1.0 + z * z).sqrt() / (bw + 2.0 * bfd * z)))
                            * area
                            + (so.sqrt() / n_cc)
                                * (5.0 / 3.0)
                                * (h_0 - bfd).powf(2.0 / 3.0)
                                * area_c)
                            / (area + area_c),
                    );
                } else if h_0 > 0.0 {
                    ck = f64::max(
                        0.0,
                        (so.sqrt() / n)
                            * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                - (2.0 / 3.0)
                                    * r.powf(5.0 / 3.0)
                                    * (2.0 * (1.0 + z * z).sqrt() / (bw + 2.0 * h_0 * z))),
                    );
                } else {
                    ck = 0.0;
                }

                km = if ck > 0.0 { f64::max(dt, dx / ck) } else { dt };

                // Calculate X parameter for h_0 (interval = 1)
                if h_0 > bfd && tw_cc > 0.0 && n_cc > 0.0 && ck > 0.0 {
                    x = f64::min(
                        0.5,
                        f64::max(0.0, 0.5 * (1.0 - (qj_0 / (2.0 * tw_cc * so * ck * dx)))),
                    );
                } else if ck > 0.0 {
                    x = f64::min(
                        0.5,
                        f64::max(0.0, 0.5 * (1.0 - (qj_0 / (2.0 * twl * so * ck * dx)))),
                    );
                } else {
                    x = 0.5;
                }

                d = km * (1.0 - x) + dt / 2.0;
                if d == 0.0 {
                    panic!("FATAL ERROR: D is 0 in MUSKINGCUNGE");
                }

                c1 = (km * x + dt / 2.0) / d;
                c2 = (dt / 2.0 - km * x) / d;
                c3 = (km * (1.0 - x) - dt / 2.0) / d;
                c4 = (ql * dt) / d;

                if wp + wp_c > 0.0 {
                    let manning_avg = ((wp * n) + (wp_c * n_cc)) / (wp + wp_c);
                    qj_0 = (c1 * qup + c2 * quc + c3 * qdp + c4)
                        - ((1.0 / manning_avg) * (area + area_c) * r.powf(2.0 / 3.0) * so.sqrt());
                }

                // Upper interval (h)
                wp_c = 0.0;
                area = 0.0;
                area_c = 0.0;

                twl = bw + 2.0 * z * h;

                if h > bfd && tw_cc > 0.0 && n_cc > 0.0 {
                    area = (bw + bfd * z) * bfd;
                    area_c = tw_cc * (h - bfd);
                    wp = bw + 2.0 * bfd * (1.0 + z * z).sqrt();
                    wp_c = tw_cc + 2.0 * (h - bfd);
                    r = (area + area_c) / (wp + wp_c);
                } else {
                    area = (bw + h * z) * h;
                    wp = bw + 2.0 * h * (1.0 + z * z).sqrt();
                    r = if wp > 0.0 { area / wp } else { 0.0 };
                }

                if h > bfd && tw_cc > 0.0 && n_cc > 0.0 {
                    ck = f64::max(
                        0.0,
                        ((so.sqrt() / n)
                            * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                - (2.0 / 3.0)
                                    * r.powf(5.0 / 3.0)
                                    * (2.0 * (1.0 + z * z).sqrt() / (bw + 2.0 * bfd * z)))
                            * area
                            + (so.sqrt() / n_cc)
                                * (5.0 / 3.0)
                                * (h - bfd).powf(2.0 / 3.0)
                                * area_c)
                            / (area + area_c),
                    );
                } else if h > 0.0 {
                    ck = f64::max(
                        0.0,
                        (so.sqrt() / n)
                            * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                - (2.0 / 3.0)
                                    * r.powf(5.0 / 3.0)
                                    * (2.0 * (1.0 + z * z).sqrt() / (bw + 2.0 * h * z))),
                    );
                } else {
                    ck = 0.0;
                }

                km = if ck > 0.0 { f64::max(dt, dx / ck) } else { dt };

                let flow_sum = c1 * qup + c2 * quc + c3 * qdp + c4;

                // Calculate X parameter for h (interval = 2)
                if h > bfd && tw_cc > 0.0 && n_cc > 0.0 && ck > 0.0 {
                    x = f64::min(
                        0.5,
                        f64::max(
                            0.25,
                            0.5 * (1.0 - (flow_sum / (2.0 * tw_cc * so * ck * dx))),
                        ),
                    );
                } else if ck > 0.0 {
                    x = f64::min(
                        0.5,
                        f64::max(0.25, 0.5 * (1.0 - (flow_sum / (2.0 * twl * so * ck * dx)))),
                    );
                } else {
                    x = 0.5;
                }

                d = km * (1.0 - x) + dt / 2.0;
                if d == 0.0 {
                    panic!("FATAL ERROR: D is 0 in MUSKINGCUNGE");
                }

                c1 = (km * x + dt / 2.0) / d;
                c2 = (dt / 2.0 - km * x) / d;
                c3 = (km * (1.0 - x) - dt / 2.0) / d;
                c4 = (ql * dt) / d;

                // Check for negative flow due to channel loss
                if c4 < 0.0 && c4.abs() > (c1 * qup + c2 * quc + c3 * qdp) {
                    c4 = -(c1 * qup + c2 * quc + c3 * qdp);
                }

                if wp + wp_c > 0.0 {
                    let manning_avg = ((wp * n) + (wp_c * n_cc)) / (wp + wp_c);
                    qj = (c1 * qup + c2 * quc + c3 * qdp + c4)
                        - ((1.0 / manning_avg) * (area + area_c) * r.powf(2.0 / 3.0) * so.sqrt());
                }

                // Update h using secant method
                if (qj_0 - qj) != 0.0 {
                    h_1 = h - (qj * (h_0 - h) / (qj_0 - qj));
                    if h_1 < 0.0 {
                        h_1 = h;
                    }
                } else {
                    h_1 = h;
                }

                if h > 0.0 {
                    rerror = ((h_1 - h) / h).abs();
                    aerror = (h_1 - h).abs();
                } else {
                    rerror = 0.0;
                    aerror = 0.9;
                }

                h_0 = f64::max(0.0, h);
                h = f64::max(0.0, h_1);
                iter += 1;

                if h < mindepth {
                    break;
                }
            }

            if iter >= maxiter {
                tries += 1;
                if tries <= 4 {
                    h = h * 1.33;
                    h_0 = h_0 * 0.67;
                    maxiter = maxiter + 25;
                    continue 'outer;
                }

                eprintln!("Musk Cunge WARNING: Failure to converge");
                eprintln!("err,iters,tries: {} {} {}", rerror, iter, tries);
            }

            // Calculate final flow
            let flow_sum = c1 * qup + c2 * quc + c3 * qdp + c4;

            if flow_sum < 0.0 {
                if c4 < 0.0 && c4.abs() > (c1 * qup + c2 * quc + c3 * qdp) {
                    qdc = 0.0;
                } else {
                    qdc = f64::max(c1 * qup + c2 * quc + c4, c1 * qup + c3 * qdp + c4);
                }
            } else {
                qdc = flow_sum;
            }

            // Calculate velocity using simplified hydraulic radius (matching Fortran)
            twl = bw + 2.0 * z * h;
            r = (h * (bw + twl) / 2.0)
                / (bw + 2.0 * (((twl - bw) / 2.0).powi(2) + h.powi(2)).sqrt());
            velc = (1.0 / n) * r.powf(2.0 / 3.0) * so.sqrt();
            depth_c = h;

            break;
        }
    } else {
        // No flow to route
        qdc = 0.0;
        velc = 0.0;
        depth_c = 0.0;
    }

    // Calculate Courant number (matching Fortran courant subroutine)
    if depth_c > 0.0 {
        let h_gt_bf = f64::max(depth_c - bfd, 0.0);
        let h_lt_bf = f64::min(bfd, depth_c);

        // Exception for NWM 3.0: if depth > bankfull but floodplain width is zero,
        // extend trapezoidal channel upwards
        let (h_gt_bf, h_lt_bf) = if h_gt_bf > 0.0 && tw_cc <= 0.0 {
            (0.0, depth_c)
        } else {
            (h_gt_bf, h_lt_bf)
        };

        let area = (bw + h_lt_bf * z) * h_lt_bf;
        let wp = bw + 2.0 * h_lt_bf * (1.0 + z * z).sqrt();
        let area_c = tw_cc * h_gt_bf;
        let wp_c = if h_gt_bf > 0.0 {
            tw_cc + 2.0 * h_gt_bf
        } else {
            0.0
        };
        let r = (area + area_c) / (wp + wp_c);

        ck = f64::max(
            0.0,
            ((so.sqrt() / n)
                * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                    - (2.0 / 3.0)
                        * r.powf(5.0 / 3.0)
                        * (2.0 * (1.0 + z * z).sqrt() / (bw + 2.0 * h_lt_bf * z)))
                * area
                + (so.sqrt() / n_cc) * (5.0 / 3.0) * h_gt_bf.powf(2.0 / 3.0) * area_c)
                / (area + area_c),
        );

        cn = ck * (dt / dx);
    }

    (qdc, velc, depth_c, ck, cn, x)
}
