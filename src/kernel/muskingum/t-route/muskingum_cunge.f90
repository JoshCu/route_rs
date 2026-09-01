module muskingum_cunge_mod

   use,intrinsic :: iso_fortran_env, only : real32
   implicit none

contains

! velp parameter completely unused
   subroutine muskingum_cunge(dt, qup, quc, qdp, ql, dx, bw, tw, twcc,&
      n, ncc, cs, s0, depthp, qdc, velc, depthc, ck, cn, X, calculate_courant)

      !* exactly follows SUBMUSKINGCUNGE in NWM:
      !* 1) qup and quc for a reach in upstream limit take zero values all the time
      !* 2) initial value of depth of time t of each reach is equal to the value at time t-1
      !* 3) qup as well as quc at time t for a downstream reach in a serial network takes
      !*    exactly the same value qdp at time t (or qdc at time t-1) for the upstream reach

      implicit none

      real(real32), intent(in) :: dt
      real(real32), intent(in) :: qup, quc, qdp, ql
      real(real32), intent(in) :: dx, bw, tw, twcc, n, ncc, cs, s0
      !real(real32), intent(in) :: velp
      real(real32), intent(in) :: depthp
      real(real32), intent(out) :: qdc, depthc
      real(real32), intent(out) :: ck, cn, X
      logical, intent(in) :: calculate_courant
      real(real32) :: z
      real(real32) :: bfd, C1, C2, C3, C4
      real(real32) :: velc

      !Uncomment next line for old initialization
      !real(real32) :: WPC, AREAC

      integer :: iter
      integer :: maxiter, tries
      real(real32) :: mindepth, aerror, rerror
      real(real32) :: R, twl, h_1, h, h_0, Qj, Qj_0



      Qj = 0.0_real32;
      Qj_0 = 0.0_real32;
      C1 = 0.0_real32;
      C2 = 0.0_real32;
      C3 = 0.0_real32;
      C4 = 0.0_real32;
      X = 0.0_real32;

      ! qdc = 0.0
      ! velc = velp
      ! depthc = depthp

      !* parameters of Secant method
      maxiter  = 100
      mindepth = 0.01_real32

      aerror = 0.01_real32
      rerror = 1.0_real32
      tries = 0

      if(cs .eq. 0.0_real32) then
         z = 1.0_real32
      else
         z = 1.0_real32/cs          !channel side distance (m)
      endif

      if(bw .gt. tw) then   !effectively infinite deep bankful
         bfd = bw/0.00001_real32
      elseif (bw .eq. tw) then
         bfd =  bw/(2.0_real32*z)  !bankfull depth is effectively
      else
         bfd =  (tw - bw)/(2.0_real32*z)  !bankfull depth (m)
      endif

      !print *, bfd
      if (n .le. 0.0_real32 .or. s0 .le. 0.0_real32 .or. z .le. 0.0_real32 .or. bw .le. 0.0_real32) then
         !print*, "Error in channel coefficients -> Muskingum cunge", n, s0, z, bw
         !call hydro_stop("In MUSKINGCUNGE() - Error in channel coefficients")
      end if

      depthc = max(depthp, 0.0_real32)
      h     = (depthc * 1.33_real32) + mindepth !1.50 of  depthc
      h_0   = (depthc * 0.67_real32)            !0.50 of depthc

      if(ql .gt. 0.0_real32 .or. qup .gt. 0.0_real32 .or. quc .gt. 0.0_real32 &
         .or. qdp .gt. 0.0_real32 .or. qdc .gt. 0.0_real32) then  !only solve if there's water to flux
110      continue

         !Uncomment next two lines for old initialization
         !WPC = 0.0_real32
         !AREAC = 0.0_real32

         iter = 0

         do while (rerror .gt. 0.01_real32 .and. aerror .ge. mindepth .and. iter .le. maxiter)

            !Uncomment next four lines for old initialization
            !call secant2_h(z, bw, bfd, twcc, s0, n, ncc, dt, dx, &
            !    qdp, ql, qup, quc, h_0, 1, WPC, Qj_0, C1, C2, C3, C4)
            !call secant2_h(z, bw, bfd, twcc, s0, n, ncc, dt, dx, &
            !    qdp, ql, qup, quc, h, 2, WPC, Qj, C1, C2, C3, C4)

            !Uncomment next four lines for new initialization
            call secant2_h(z, bw, bfd, twcc, s0, n, ncc, dt, dx, &
               qdp, ql, qup, quc, h_0, 1, Qj_0, C1, C2, C3, C4, X)
            call secant2_h(z, bw, bfd, twcc, s0, n, ncc, dt, dx, &
               qdp, ql, qup, quc, h, 2, Qj, C1, C2, C3, C4, X)

            if(Qj_0-Qj .ne. 0.0_real32) then
               h_1 = h - ((Qj * (h_0 - h))/(Qj_0 - Qj)) !update h, 3rd estimate

               if(h_1 .lt. 0.0_real32) then
                  h_1 = h
               endif
            else
               h_1 = h
            endif

            if(h .gt. 0.0_real32) then
               rerror = abs((h_1 - h)/h) !relative error is new estimate and 2nd estimate
               aerror = abs(h_1 -h)      !absolute error
            else
               rerror = 0.0_real32
               aerror = 0.9_real32
            endif

            h_0  = max(0.0_real32,h)
            h    = max(0.0_real32,h_1)
            iter = iter + 1
            !write(41,"(3i5,2x,8f15.4)") k, i, iter, dmy1, Qj_0, dmy2, Qj, h_0, h, rerror, aerror
            !write(42,*) k, i, iter, dmy1, Qj_0, dmy2, Qj, h_0, h, rerror, aerror
            if( h .lt. mindepth) then  ! exit loop if depth is very small
               goto 111
            endif
         end do !*do while (rerror .gt. 0.01 .and. ....
111      continue

         if(iter .ge. maxiter) then
            tries = tries + 1

            if(tries .le. 4) then  ! expand the search space
               h     =  h * 1.33_real32
               h_0   =  h_0 * 0.67_real32
               maxiter = maxiter + 25 !and increase the number of allowable iterations
               goto 110
            endif
            !print*, "Musk Cunge WARNING: Failure to converge"
            !print*, 'RouteLink index:', idx + linkls_s(my_id+1) - 1
            !print*, "id,err,iters,tries",PC*ncc))/(WP+WPC))) * &
            !        (AREA+AREAC) * (R**(2./3.)) * sqrt(s0)) idx, rerror, iter, tries
            !print*, "Ck,X,dt,Km",Ck,X,dt,Km
            !print*, "s0,dx,h",s0,dx,h
            !print*, "qup,quc,qdp,ql", qup,quc,qdp,ql
            !print*, "bfd,bw,tw,twl", bfd,bw,tw,twl
            !print*, "Qmc,Qmn", (C1*qup)+(C2*quc)+(C3*qdp) + C4,((1/(((WP*n)+(WPC*ncc))/(WP+WPC))) * &
            !        (AREA+AREAC) * (R**(2./3.)) * sqrt(s0))
         endif

         !*yw added for test
         !*DY and LKR Added to update for channel loss
         if(((C1*qup)+(C2*quc)+(C3*qdp) + C4) .lt. 0.0_real32) then
            if( (C4 .lt. 0.0_real32) .and. (abs(C4) .gt. (C1*qup)+(C2*quc)+(C3*qdp)) )  then ! channel loss greater than water in chan
               qdc = 0.0_real32
               !qdc = -111.1
            else
               qdc = MAX( ( (C1*qup)+(C2*quc) + C4),((C1*qup)+(C3*qdp) + C4) )
               !qdc = -222.2
            endif
         else
            qdc = ((C1*qup)+(C2*quc)+(C3*qdp) + C4) !-- pg 295 Bedient huber
            !write(*,*)"C1", C1, "qup", qup, "C2", C2, "quc", quc, "C3", C3, "qdp", qdp, "C4", C4
            !qdc = -333.3
         endif

         call hydraulic_geometry(h, bfd, bw, twcc, z, twl, R)
         !TODO: The following line allows the system to reproduce the current
         !velocity calculation, however the hydraulic radius provided is not
         !taking into account the flood-plan flow, nor is the velocity
         !accouting for the variation in Manning n.
         R = (h*(bw + twl) / 2.0_real32) / (bw + 2.0_real32*(((twl - bw) / 2.0_real32)**2.0_real32 + h**2.0_real32)**0.5_real32)
         velc = (1.0_real32/n) * (R **(2.0_real32/3.0_real32)) * sqrt(s0)  !*average velocity in m/s
         depthc = h
      else   !*no flow to route
         qdc = 0.0_real32
         cn = 0.0_real32
         ck = 0.0_real32
         !qdc = -444.4
         velc = 0.0_real32
         depthc = 0.0_real32
      end if !*if(ql .gt. 0.0 .or. ...

      ! *************************************************************
      ! call courant subroutine here
      ! *************************************************************
      if (calculate_courant) then
         call courant(h, bfd, bw, twcc, ncc, s0, n, z, dx, dt, ck, cn)
      end if
      !print*, "deep down", depthc

   end subroutine muskingum_cunge

!**---------------------------------------------------**!
!*                                                     *!
!*                 SECANT2 SUBROUTINE                  *!
!*                                                     *!
!**---------------------------------------------------**!
!Uncomment this function signature for old initialization
!subroutine secant2_h(z, bw, bfd, twcc, s0, n, ncc, dt, dx, &
!    qdp, ql, qup, quc, h, interval, WPC, Qj, C1, C2, C3, C4)

!Uncomment this function signature for new initialization
   subroutine secant2_h(z, bw, bfd, twcc, s0, n, ncc, dt, dx, &
      qdp, ql, qup, quc, h, interval, Qj, C1, C2, C3, C4, X)

      implicit none

      real(real32), intent(in) :: z, bw, bfd, twcc, s0, n, ncc
      real(real32), intent(in) :: dt, dx
      real(real32), intent(in) :: qdp, ql, qup, quc
      real(real32), intent(in) :: h
      real(real32), intent(inout) :: Qj, C1, C2, C3, C4, X
      integer,    intent(in) :: interval

      real(real32) :: twl, AREA, WP, R
      real(real32) :: Ck, Cn, Km, D
      integer    :: upper_interval, lower_interval

      !Uncomment for old initialization
      !real(real32), intent(out) :: WPC
      !real(real32) :: AREAC
      !Uncomment for new initialization
      real(real32) :: WPC, AREAC

      twl = 0.0_real32
      WP = 0.0_real32

      !Uncomment next line for old initialization
      !AREA = 0.0_real32
      !Uncomment next two lines for new initialization
      WPC = 0.0_real32
      AREA = 0.0_real32
      AREAC = 0.0_real32

      R = 0.0_real32
      Ck = 0.0_real32
      Cn = 0.0_real32

      Km = 0.0_real32
      X = 0.0_real32
      D = 0.0_real32



      !--upper interval -----------
      upper_interval = 1
      !--lower interval -----------
      lower_interval = 2


      call hydraulic_geometry(h, bfd, bw, twcc, z, &
         twl, R, AREA, AREAC, WP, WPC)

      !**kinematic celerity, Ck
      if( (h .gt. bfd) .and. (twcc .gt. 0.0_real32) .and. (ncc .gt. 0.0_real32) ) then
         !*water outside of defined channel weight the celerity by the contributing area, and
         !*assume that the mannings of the spills is 2x the manning of the channel
         Ck = max(0.0_real32,((sqrt(s0)/n) &
            * ((5.0_real32/3.0_real32)*R**(2.0_real32/3.0_real32) &
            - ((2.0_real32/3.0_real32)*R**(5.0_real32/3.0_real32) &
            * (2.0_real32*sqrt(1.0_real32 + z*z)/(bw+2.0_real32*bfd*z)))) &
            * AREA &
            + ((sqrt(s0)/(ncc))*(5.0_real32/3.0_real32) &
            * (h-bfd)**(2.0_real32/3.0_real32))*AREAC) &
            / (AREA+AREAC))
      else
         if(h .gt. 0.0_real32) then !avoid divide by zero
            Ck = max(0.0_real32,(sqrt(s0)/n) &
               * ((5.0_real32/3.0_real32)*R**(2.0_real32/3.0_real32) &
               - ((2.0_real32/3.0_real32)*R**(5.0_real32/3.0_real32) &
               * (2.0_real32*sqrt(1.0_real32 + z*z)/(bw+2.0_real32*h*z)))))
         else
            Ck = 0.0_real32
         endif
      endif

      !**MC parameter, K
      if(Ck .gt. 0.0_real32) then
         Km = max(dt,dx/Ck)
      else
         Km = dt
      endif

      !**MC parameter, X
      if( (h .gt. bfd) .and. (twcc .gt. 0.0_real32) .and. (ncc .gt. 0.0_real32) .and. (Ck .gt. 0.0_real32) ) then !water outside of defined channel
         !H0
         if (interval .eq. upper_interval) then
            X = min(0.5_real32,max(0.0_real32,0.5_real32*(1.0_real32-(Qj/(2.0_real32*twcc*s0*Ck*dx)))))
         endif
         if (interval .eq. lower_interval) then
            !H
            X = min(0.5_real32,max(0.25_real32,0.5_real32*(1.0_real32-(((C1*qup)+(C2*quc)+(C3*qdp) + C4) &
               /(2.0_real32*twcc*s0*Ck*dx)))))
         endif
      else
         if(Ck .gt. 0.0_real32) then
            !H0
            if (interval .eq. upper_interval) then
               X = min(0.5_real32,max(0.0_real32,0.5_real32*(1.0_real32-(Qj/(2.0_real32*twl*s0*Ck*dx)))))
            endif
            !H
            if (interval .eq. lower_interval) then
               X = min(0.5_real32,max(0.25_real32,0.5_real32*(1.0_real32-(((C1*qup)+(C2*quc)+(C3*qdp) + C4) &
                  /(2.0_real32*twl*s0*Ck*dx)))))
            endif
         else
            X = 0.5_real32
         endif
      endif

      !write(45,"(3i5,2x,4f10.3)") gk, gi, idx, h, Ck, Km, X
      D = (Km*(1.0_real32 - X) + dt/2.0_real32)              !--seconds
      if(D .eq. 0.0_real32) then
         !print *, "FATAL ERROR: D is 0 in MUSKINGCUNGE", Km, X, dt,D
         !call hydro_stop("In MUSKINGCUNGE() - D is 0.")
      endif

      C1 =  (Km*X + dt/2.0_real32)/D
      C2 =  (dt/2.0_real32 - Km*X)/D
      C3 =  (Km*(1.0_real32-X)-dt/2.0_real32)/D
      C4 =  (ql*dt)/D

      !H
      if (interval .eq. lower_interval) then
         if( (C4 .lt. 0.0_real32) .and. (abs(C4) .gt. (C1*qup)+(C2*quc)+(C3*qdp)))  then
            C4 = -((C1*qup)+(C2*quc)+(C3*qdp))
         endif
      endif
      !!Uncomment to show WP/WPC behavior above bankfull
      !if (interval .eq. upper_interval) then
      !    print *,"secant1 --", "WP:", WP, "WPC:", WPC
      !else
      !    print *,"secant2 --", "WP:", WP, "WPC:", WPC
      !endif

      if((WP+WPC) .gt. 0.0_real32) then  !avoid divide by zero
         Qj =  ((C1*qup)+(C2*quc)+(C3*qdp) + C4) - ((1.0_real32/(((WP*n)+(WPC*ncc))/(WP+WPC))) * &
            (AREA+AREAC) * (R**(2.0_real32/3.0_real32)) * sqrt(s0)) !f(x)
      else
         Qj = 0.0_real32
      endif

   end subroutine secant2_h


!**---------------------------------------------------**!
!*                                                     *!
!*                 COURANT SUBROUTINE                  *!
!*                                                     *!
!**---------------------------------------------------**!
   subroutine courant(h, bfd, bw, twcc, ncc, s0, n, z, dx, dt, ck, cn)

      implicit none

      real(real32), intent(in) :: h, bfd, bw, twcc, z
      real(real32), intent(in) :: ncc, s0, n, dx, dt
      real(real32), intent(out) :: ck
      real(real32), intent(out) :: cn
      real(real32) :: h_gt_bf, h_lt_bf, AREA, AREAC, WP, WPC, R
      real(real32) :: twl !UNUSED -- needed only for hydraulic_geometry call

      call hydraulic_geometry(h, bfd, bw, twcc, z, &
         twl, R, AREA, AREAC, WP, WPC, h_lt_bf, h_gt_bf)

      ck = max(0.0_real32,((sqrt(s0)/n) &
         * ((5.0_real32/3.0_real32)*R**(2.0_real32/3.0_real32) &
         - ((2.0_real32/3.0_real32)*R**(5.0_real32/3.0_real32) &
         * (2.0_real32*sqrt(1.0_real32 + z*z)/(bw+2.0_real32*h_lt_bf*z)))) &
         * AREA &
         + ((sqrt(s0)/(ncc))*(5.0_real32/3.0_real32) &
         * (h_gt_bf)**(2.0_real32/3.0_real32))*AREAC) &
         / (AREA+AREAC))

      cn = ck * (dt/dx)

   end subroutine courant

!**---------------------------------------------------**!
!*                                                     *!
!*           Hydraulic Geometry SUBROUTINE             *!
!*                                                     *!
!**---------------------------------------------------**!
   subroutine hydraulic_geometry(h, bfd, bw, twcc, z, &
      twl, R, AREA, AREAC, WP, WPC, h_lt_bf, h_gt_bf)

      implicit none

      real(real32), intent(in) :: h, bfd, bw, twcc, z
      real(real32), intent(out), optional :: twl, R, AREA, AREAC, WP, WPC
      real(real32) :: twl_loc, R_loc, AREA_loc, AREAC_loc, WP_loc, WPC_loc
      real(real32), intent(out), optional :: h_gt_bf, h_lt_bf
      real(real32) :: h_gt_bf_loc, h_lt_bf_loc

      twl_loc = 0.0_real32
      R_loc = 0.0_real32
      AREA_loc = 0.0_real32
      AREAC_loc = 0.0_real32
      WP_loc = 0.0_real32
      WPC_loc = 0.0_real32

      twl_loc = bw + 2.0_real32*z*h

      h_gt_bf_loc = max(h - bfd, 0.0_real32)
      h_lt_bf_loc = min(bfd, h)

      ! Exception for NWM 3.0 channel geometry:
      ! if depth is beyond bankfull, but the floodplain width is zero,
      ! then just extend the trapezoidal channel upwards beyond bankfull
      if ( (h_gt_bf_loc .gt. 0.0_real32) .and. (twcc .le. 0.0_real32) ) then
         h_gt_bf_loc = 0.0_real32
         h_lt_bf_loc = h
      endif

      AREA_loc = (bw + h_lt_bf_loc * z ) * h_lt_bf_loc

      WP_loc = (bw + 2 * h_lt_bf_loc * sqrt(1 + z*z))

      AREAC_loc = (twcc * h_gt_bf_loc)

      if(h_gt_bf_loc .gt. 0.0_real32) then
         WPC_loc = twcc + (2 * (h_gt_bf_loc))
      else
         WPC_loc = 0
      endif

      R_loc   = (AREA_loc + AREAC_loc)/(WP_loc + WPC_loc)
      !R = (h*(bw + twl) / 2.0_real32) / (bw + 2.0_real32*(((twl - bw) / 2.0_real32)**2.0_real32 + h**2.0_real32)**0.5_real32)
      if (present(twl)) then
         twl = twl_loc
      endif
      if (present(R)) then
         R = R_loc
      endif
      if (present(AREA)) then
         AREA = AREA_loc
      endif
      if (present(AREAC)) then
         AREAC = AREAC_loc
      endif
      if (present(WP)) then
         WP = WP_loc
      endif
      if (present(WPC)) then
         WPC = WPC_loc
      endif
      if (present(h_gt_bf)) then
         h_gt_bf = h_gt_bf_loc
      endif
      if (present(h_lt_bf)) then
         h_lt_bf = h_lt_bf_loc
      endif

   end subroutine hydraulic_geometry


end module muskingum_cunge_mod
