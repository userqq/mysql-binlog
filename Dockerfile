FROM php:8.2-cli-buster

WORKDIR /app/

RUN apt update \
    && apt install -y libgmpxx4ldbl libgmp-dev libffi-dev zlib1g-dev \
    && docker-php-ext-configure ffi --with-ffi \
    && docker-php-ext-install gmp ffi opcache bcmath pcntl \
    && echo "zend.assertions=-1" >> /usr/local/etc/php/conf.d/docker-php.ini \
    && echo "opcache.enable=1" >> /usr/local/etc/php/conf.d/docker-php-ext-opcache.ini \
    && echo "opcache.enable_cli=1" >> /usr/local/etc/php/conf.d/docker-php-ext-opcache.ini \
    && echo "opcache.jit=1254" >> /usr/local/etc/php/conf.d/docker-php-ext-opcache.ini \
    && echo "opcache.jit_buffer_size=100M" >> /usr/local/etc/php/conf.d/docker-php-ext-opcache.ini \
    && echo "ffi.enable=preload" >> /usr/local/etc/php/conf.d/docker-php-ext-ffi.ini

RUN pecl install ev-1.1.5 eio-3.0.0RC4 \
    && echo "extension=eio.so" >> /usr/local/etc/php/conf.d/docker-php-ext-eio.ini \
    && echo "extension=ev.so" >> /usr/local/etc/php/conf.d/docker-php-ext-ev.ini

RUN cd /tmp/ \
    && apt install -y zlib1g-dev git \
    && git clone https://github.com/NoiseByNorthwest/php-spx.git \
    && cd php-spx \
    && phpize && ./configure && make && make install && rm -rf /tmp/php-spx \
    && echo "extension=spx.so" >> /usr/local/etc/php/conf.d/docker-php-ext-spx.ini

CMD bin/run
