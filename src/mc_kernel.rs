/// Muskingcunge routing implementation for channel flow calculations
/// Ported from Fortran to Rust
pub fn submuskingcunge(
    qup: f64,     // flow upstream previous timestep
    quc: f64,     // flow upstream current timestep
    qdp: f64,     // flow downstream previous timestep
    ql: f64,      // lateral inflow through reach (m^3/sec)
    dt: f64,      // routing period in seconds
    so: f64,      // channel bottom slope %
    dx: f64,      // channel length (m)
    n: f64,       // mannings coefficient
    cs: f64,      // channel side slope
    bw: f64,      // bottom width (meters)
    tw: f64,      // top width before bankfull (meters)
    tw_cc: f64,   // top width of compound (meters)
    n_cc: f64,    // mannings of compound
    depth_p: f64, // depth of flow in channel
) -> (f64, f64, f64) {
    // Returns (qdc, velc, depthc)
    // Local variables
    let mut c1: f64 = 0.0;
    let mut c2: f64 = 0.0;
    let mut c3: f64 = 0.0;
    let mut c4: f64 = 0.0;
    let mut km: f64 = 0.0; // K travel time in hrs in reach
    let mut x: f64 = 0.0; // weighting factors 0<=X<=0.5
    let mut ck: f64 = 0.0; // wave celerity (m/s)

    // Channel geometry and characteristics, local variables
    let mut twl: f64 = 0.0; // top width at simulated flow (m)
    let mut area: f64 = 0.0; // Cross sectional area channel
    let mut area_c: f64 = 0.0; // Cross sectional area compound
    let z: f64; // trapezoid distance (m)
    let mut r: f64 = 0.0; // Hydraulic radius
    let mut wp: f64 = 0.0; // wetted perimeter
    let mut wp_c: f64 = 0.0; // wetted perimeter of compound
    let mut h: f64; // depth of flow in channel
    let mut h_0: f64; // secant method estimate
    let mut h_1: f64; // secant method estimate
    let bfd: f64; // bankfull depth (m)
    let mut qj_0: f64 = 0.0; // secant method estimate
    let mut qj: f64 = 0.0; // intermediate flow estimate
    let mut d: f64; // diffusion coeff
    let mut aerror: f64; // absolute error
    let mut rerror: f64 = 1.0; // relative error
    let mut iter: i32; // iteration counter
    let mut maxiter: i32 = 100; // maximum number of iterations
    let mindepth: f64 = 0.01; // minimum depth in channel
    let mut tries: i32 = 0; // channel segment counter

    aerror = 0.01;

    // Set trapezoid distance
    if cs == 0.0 {
        z = 1.0;
    } else {
        z = 1.0 / cs; // channel side distance (m)
    }

    // Calculate bankfull depth
    if bw > tw {
        // Effectively infinite deep bankful
        bfd = bw / 0.00001;
    } else if bw == tw {
        bfd = bw / (2.0 * z); // bankfull depth is effectively
    } else {
        bfd = (tw - bw) / (2.0 * z); // bankfull depth (m)
    }

    // Check for invalid channel coefficients
    if n <= 0.0 || so <= 0.0 || z <= 0.0 || bw <= 0.0 {
        panic!(
            "Error in channel coefficients -> Muskingum cunge: n={}, so={}, z={}, bw={}",
            n, so, z, bw
        );
    }

    // Initialize depth
    let mut depth_c = f64::max(depth_p, 0.0);
    h = (depth_c * 1.33) + mindepth; // 1.50 of depth
    h_0 = depth_c * 0.67; // 0.50 of depth

    let qdc: f64; // flow downstream current timestep
    let mut velc: f64 = 0.0; // channel velocity

    // Only solve if there's water to flux
    if ql > 0.0 || qup > 0.0 || qdp > 0.0 {
        'outer: loop {
            iter = 0;

            // Secant method loop
            while rerror > 0.01 && aerror >= mindepth && iter <= maxiter {
                area_c = 0.0;
                wp_c = 0.0;

                // ----- Lower interval --------------------
                twl = bw + 2.0 * z * h_0; // Top surface water width of the channel inflow

                if h_0 > bfd {
                    // Water outside of defined channel
                    area = (bw + bfd * z) * bfd;
                    area_c = tw_cc * (h_0 - bfd); // Assume compound component is rect. chan
                    wp = bw + 2.0 * bfd * f64::sqrt(1.0 + z * z);
                    wp_c = tw_cc + (2.0 * (h_0 - bfd)); // WPC is 2 times the Tw
                    r = (area + area_c) / (wp + wp_c); // Hydraulic radius
                } else {
                    area = (bw + h_0 * z) * h_0;
                    wp = bw + 2.0 * h_0 * f64::sqrt(1.0 + z * z);

                    if wp > 0.0 {
                        r = area / wp;
                    } else {
                        r = 0.0;
                    }
                }

                if h_0 > bfd {
                    // Water outside of defined channel
                    // Weight the celerity by the contributing area
                    ck = f64::max(
                        0.0,
                        ((f64::sqrt(so) / n)
                            * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                - ((2.0 / 3.0)
                                    * r.powf(5.0 / 3.0)
                                    * (2.0 * f64::sqrt(1.0 + z * z) / (bw + 2.0 * bfd * z))))
                            * area
                            + ((f64::sqrt(so) / n_cc) * (5.0 / 3.0) * (h_0 - bfd).powf(2.0 / 3.0))
                                * area_c)
                            / (area + area_c),
                    );
                } else {
                    if h_0 > 0.0 {
                        ck = f64::max(
                            0.0,
                            (f64::sqrt(so) / n)
                                * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                    - ((2.0 / 3.0)
                                        * r.powf(5.0 / 3.0)
                                        * (2.0 * f64::sqrt(1.0 + z * z) / (bw + 2.0 * h_0 * z)))),
                        );
                    } else {
                        ck = 0.0;
                    }
                }

                if ck > 0.0 {
                    km = f64::max(dt, dx / ck);
                } else {
                    km = dt;
                }

                if h_0 > bfd {
                    // Water outside of defined channel
                    x = f64::min(
                        0.5,
                        f64::max(0.0, 0.5 * (1.0 - (qj_0 / (2.0 * tw_cc * so * ck * dx)))),
                    );
                } else {
                    if ck > 0.0 {
                        x = f64::min(
                            0.5,
                            f64::max(0.0, 0.5 * (1.0 - (qj_0 / (2.0 * twl * so * ck * dx)))),
                        );
                    } else {
                        x = 0.5;
                    }
                }

                d = km * (1.0 - x) + dt / 2.0; // Seconds
                if d == 0.0 {
                    panic!(
                        "FATAL ERROR: D is 0 in MUSKINGCUNGE: km={}, x={}, dt={}, d={}",
                        km, x, dt, d
                    );
                }

                c1 = (km * x + dt / 2.0) / d;
                c2 = (dt / 2.0 - km * x) / d;
                c3 = (km * (1.0 - x) - dt / 2.0) / d;
                c4 = (ql * dt) / d;

                if (wp + wp_c) > 0.0 {
                    // Avoid divide by zero
                    let manning_avg = ((wp * n) + (wp_c * n_cc)) / (wp + wp_c);
                    qj_0 = ((c1 * qup) + (c2 * quc) + (c3 * qdp) + c4)
                        - ((1.0 / manning_avg)
                            * (area + area_c)
                            * r.powf(2.0 / 3.0)
                            * f64::sqrt(so));
                }

                area_c = 0.0;
                wp_c = 0.0;

                // --Upper interval -----------
                twl = bw + 2.0 * z * h; // Top width of the channel inflow

                if h > bfd {
                    // Water outside of defined channel
                    area = (bw + bfd * z) * bfd;
                    area_c = tw_cc * (h - bfd); // Assume compound component is rect. chan
                    wp = bw + 2.0 * bfd * f64::sqrt(1.0 + z * z);
                    wp_c = tw_cc + (2.0 * (h - bfd)); // The additional wetted perimeter
                    r = (area + area_c) / (wp + wp_c);
                } else {
                    area = (bw + h * z) * h;
                    wp = bw + 2.0 * h * f64::sqrt(1.0 + z * z);
                    if wp > 0.0 {
                        r = area / wp;
                    } else {
                        r = 0.0;
                    }
                }

                if h > bfd {
                    // Water outside of defined channel, assumed rectangular
                    ck = f64::max(
                        0.0,
                        ((f64::sqrt(so) / n)
                            * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                - ((2.0 / 3.0)
                                    * r.powf(5.0 / 3.0)
                                    * (2.0 * f64::sqrt(1.0 + z * z) / (bw + 2.0 * bfd * z))))
                            * area
                            + ((f64::sqrt(so) / n_cc) * (5.0 / 3.0) * (h - bfd).powf(2.0 / 3.0))
                                * area_c)
                            / (area + area_c),
                    );
                } else {
                    if h > 0.0 {
                        ck = f64::max(
                            0.0,
                            (f64::sqrt(so) / n)
                                * ((5.0 / 3.0) * r.powf(2.0 / 3.0)
                                    - ((2.0 / 3.0)
                                        * r.powf(5.0 / 3.0)
                                        * (2.0 * f64::sqrt(1.0 + z * z) / (bw + 2.0 * h * z)))),
                        );
                    } else {
                        ck = 0.0;
                    }
                }

                if ck > 0.0 {
                    km = f64::max(dt, dx / ck);
                } else {
                    km = dt;
                }

                let flow_sum = (c1 * qup) + (c2 * quc) + (c3 * qdp) + c4;

                if h > bfd {
                    // Water outside of defined channel
                    x = f64::min(
                        0.5,
                        f64::max(
                            0.25,
                            0.5 * (1.0 - (flow_sum / (2.0 * tw_cc * so * ck * dx))),
                        ),
                    );
                } else {
                    if ck > 0.0 {
                        x = f64::min(
                            0.5,
                            f64::max(0.25, 0.5 * (1.0 - (flow_sum / (2.0 * twl * so * ck * dx)))),
                        );
                    } else {
                        x = 0.5;
                    }
                }

                d = km * (1.0 - x) + dt / 2.0; // Seconds
                if d == 0.0 {
                    panic!(
                        "FATAL ERROR: D is 0 in MUSKINGCUNGE: km={}, x={}, dt={}, d={}",
                        km, x, dt, d
                    );
                }

                c1 = (km * x + dt / 2.0) / d;
                c2 = (dt / 2.0 - km * x) / d;
                c3 = (km * (1.0 - x) - dt / 2.0) / d;
                c4 = (ql * dt) / d;

                // Check for negative flow due to channel loss
                if c4 < 0.0 && f64::abs(c4) > (c1 * qup) + (c2 * quc) + (c3 * qdp) {
                    c4 = -((c1 * qup) + (c2 * quc) + (c3 * qdp));
                }

                if (wp + wp_c) > 0.0 {
                    let manning_avg = ((wp * n) + (wp_c * n_cc)) / (wp + wp_c);
                    qj = ((c1 * qup) + (c2 * quc) + (c3 * qdp) + c4)
                        - ((1.0 / manning_avg)
                            * (area + area_c)
                            * r.powf(2.0 / 3.0)
                            * f64::sqrt(so));
                }

                if (qj_0 - qj) != 0.0 {
                    h_1 = h - ((qj * (h_0 - h)) / (qj_0 - qj)); // Update h, 3rd estimate
                    if h_1 < 0.0 {
                        h_1 = h;
                    }
                } else {
                    h_1 = h;
                }

                if h > 0.0 {
                    rerror = f64::abs((h_1 - h) / h); // Relative error between new estimate and 2nd estimate
                    aerror = f64::abs(h_1 - h); // Absolute error
                } else {
                    rerror = 0.0;
                    aerror = 0.9;
                }

                h_0 = f64::max(0.0, h);
                h = f64::max(0.0, h_1);
                iter += 1;

                if h < mindepth {
                    // Exit loop if depth is very small
                    break;
                }
            }

            if iter >= maxiter {
                tries += 1;
                if tries <= 4 {
                    // Expand the search space
                    h = h * 1.33;
                    h_0 = h_0 * 0.67;
                    maxiter = maxiter + 25; // Increase the number of allowable iterations
                    continue 'outer;
                }

                eprintln!("Musk Cunge WARNING: Failure to converge");
                eprintln!("err,iters,tries: {} {} {}", rerror, iter, tries);
                eprintln!("Ck,X,dt,Km: {} {} {} {}", ck, x, dt, km);
                eprintln!("So,dx,h: {} {} {}", so, dx, h);
                eprintln!("qup,quc,qdp,ql: {} {} {} {}", qup, quc, qdp, ql);
                eprintln!("bfd,Bw,Tw,Twl: {} {} {} {}", bfd, bw, tw, twl);

                let flow_sum = (c1 * qup) + (c2 * quc) + (c3 * qdp) + c4;
                let manning_avg = ((wp * n) + (wp_c * n_cc)) / (wp + wp_c);
                let manning_term =
                    (1.0 / manning_avg) * (area + area_c) * r.powf(2.0 / 3.0) * f64::sqrt(so);

                eprintln!("Qmc,Qmn: {} {}", flow_sum, manning_term);
            }

            // Calculate flow
            let flow_sum = (c1 * qup) + (c2 * quc) + (c3 * qdp) + c4;

            if flow_sum < 0.0 {
                if c4 < 0.0 && f64::abs(c4) > (c1 * qup) + (c2 * quc) + (c3 * qdp) {
                    // Channel loss greater than water in channel
                    qdc = 0.0;
                } else {
                    qdc = f64::max((c1 * qup) + (c2 * quc) + c4, (c1 * qup) + (c3 * qdp) + c4);
                }
            } else {
                qdc = flow_sum; // pg 295 Bedient huber
            }

            twl = bw + (2.0 * z * h);
            r = (h * (bw + twl) / 2.0)
                / (bw + 2.0 * f64::sqrt(((twl - bw) / 2.0).powf(2.0) + h.powf(2.0)));
            velc = (1.0 / n) * r.powf(2.0 / 3.0) * f64::sqrt(so); // Average velocity in m/s
            depth_c = h;

            break;
        }
    } else {
        // No flow to route
        qdc = 0.0;
        depth_c = 0.0;
    }

    // Return the calculated values
    (qdc, velc, depth_c)
}
