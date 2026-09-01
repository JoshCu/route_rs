! TODO: Move shared structs to a shared mod
subroutine c_binding_muskingum_cunge_t_route(input, result, calculate_courant) bind(c)

    use, intrinsic :: iso_c_binding, only: c_int, c_float, c_ptr, c_f_pointer, c_loc, c_bool
    use, intrinsic :: iso_fortran_env, only: real32
    use muskingum_cunge_mod, only: muskingum_cunge
    !use precis
    implicit none

    type, bind(c) :: muskingum_cunge_input_t
        real(real32) :: dt      ! routing period in  seconds
        real(real32) :: qup     ! flow upstream previous timestep
        real(real32) :: quc     ! flow upstream current timestep
        real(real32) :: qdp     ! flow downstream previous timestep
        real(real32) :: ql      ! lateral inflow through reach (m^3/sec)
        real(real32) :: dx      ! channel lngth (m)
        real(real32) :: bw      ! bottom width (meters)
        real(real32) :: tw      ! top width before bankfull (meters)
        real(real32) :: tw_cc   ! top width of Compund (meters)
        real(real32) :: n       ! mannings coefficient
        real(real32) :: n_cc    ! mannings of compund
        real(real32) :: cs      ! Channel side slope slope
        real(real32) :: s0      ! Channel bottom slope %
        real(real32) :: velp    ! UNUSED 
        real(real32) :: depthp  ! depth of flow in channel
    end type

    type, bind(c) :: muskingum_cunge_result_t
        real(real32) :: qdc     ! flow downstream current timestep
        real(real32) :: velc    ! channel velocity
        real(real32) :: depthc  ! depth of flow in channel
        real(real32) :: ck  
        real(real32) :: cn 
        real(real32) :: X
    end type

    ! strictly typed inputs from calling code, passed by value
    type(muskingum_cunge_input_t), intent(inout) :: input
    type(muskingum_cunge_result_t), intent(inout) :: result
    logical(c_bool), intent(in), value :: calculate_courant

    ! muskingum_cunge(dt, qup, quc, qdp, ql, dx, bw, tw, twcc, n, ncc, cs, s0, depthp, qdc, depthc, ck, cn, X, calculate_courant)
    call muskingum_cunge(input%dt, input%qup, input%quc, input%qdp, input%ql,input%dx, input%bw,&
                        input%tw, input%tw_cc, input%n, input%n_cc, input%cs,input%s0,input%depthp,&
                        result%qdc, result%velc, result%depthc, result%ck, result%cn, result%X,&
                        logical(calculate_courant))
end subroutine

subroutine c_binding_muskingum_cunge_t_route_legacy(input, result, calculate_courant) bind(c)

    use, intrinsic :: iso_c_binding, only: c_int, c_float, c_ptr, c_f_pointer, c_loc, c_bool
    use, intrinsic :: iso_fortran_env, only: real32
    use muskingcunge_module, only: muskingcungenwm
    use precis
    implicit none

    type, bind(c) :: muskingum_cunge_input_t
        real(real32) :: dt      ! routing period in  seconds
        real(real32) :: qup     ! flow upstream previous timestep
        real(real32) :: quc     ! flow upstream current timestep
        real(real32) :: qdp     ! flow downstream previous timestep
        real(real32) :: ql      ! lateral inflow through reach (m^3/sec)
        real(real32) :: dx      ! channel lngth (m)
        real(real32) :: bw      ! bottom width (meters)
        real(real32) :: tw      ! top width before bankfull (meters)
        real(real32) :: tw_cc   ! top width of Compund (meters)
        real(real32) :: n       ! mannings coefficient
        real(real32) :: n_cc    ! mannings of compund
        real(real32) :: cs      ! Channel side slope slope
        real(real32) :: s0      ! Channel bottom slope %
        real(real32) :: velp    ! UNUSED 
        real(real32) :: depthp  ! depth of flow in channel
    end type

    ! Brodie A: ck, cn, X do not exist in MUSKINGCUNGE.f90 params list. They do exist in the py bindings output list. Why?
    type, bind(c) :: muskingum_cunge_result_t
        real(real32) :: qdc     ! flow downstream current timestep
        real(real32) :: velc    ! channel velocity
        real(real32) :: depthc  ! depth of flow in channel
        real(real32) :: ck  
        real(real32) :: cn 
        real(real32) :: X
    end type

    type(muskingum_cunge_input_t), intent(inout) :: input
    type(muskingum_cunge_result_t), intent(inout) :: result
    logical(c_bool), intent(in), value :: calculate_courant

    !muskingcungenwm(dt, qup, quc, qdp, ql, dx, bw, tw, twcc,n, ncc, cs, s0, velp, depthp, qdc, velc, depthc, ck, cn, X, calculate_courant)
    call muskingcungenwm(input%dt, input%qup, input%quc, input%qdp, input%ql, input%dx,&
        input%bw, input%tw, input%tw_cc, input%n, input%n_cc, input%cs, input%s0, input%velp, input%depthp,&
        result%qdc, result%velc, result%depthc, result%ck, result%cn, result%X,&
        logical(calculate_courant))

end subroutine