# install python3-devel
yum install -y gfortran python3-devel

# install openmp for alibaba clang 11
yum install -b current -y libomp11-devel libomp11

# install Intel MKL
yum install -y ca-certificates
yum-config-manager --add-repo https://yum.repos.intel.com/mkl/setup/intel-mkl.repo
rpm --import https://yum.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB
yum install -y intel-mkl-64bit-2020.0-088
