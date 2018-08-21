/* eslint-disable no-console */
import gulp from 'gulp';
import autoprefixer from 'autoprefixer';
import eslint from 'gulp-eslint';
import rimraf from 'rimraf';
import browserSync, { reload } from 'browser-sync';
import sourcemaps from 'gulp-sourcemaps';
import postcss from 'gulp-postcss';
import nested from 'postcss-nested';
import vars from 'postcss-simple-vars';
import extend from 'postcss-simple-extend';
import cssnano from 'cssnano';
import runSequence from 'run-sequence';
import webpack from 'webpack';
import webpackDevMiddleware from 'webpack-dev-middleware';
import webpackHotMiddleware from 'webpack-hot-middleware';
import config from './config';

const paths = {
  bundle: 'app.js',
  srcJsx: 'src/app.js',
  srcServer: 'src/server.js',
  srcCss: 'src/**/*.css',
  srcFonts: 'src/fonts/**',
  srcImg: 'src/images/**',
  srcPublic: 'src/public/**',
  srcLint: ['src/**/*.js', 'test/**/*.js'],
  dist: 'dist/public'
};

gulp.task('clean', cb => {
  rimraf('dist', cb);
});

gulp.task('browserSync', () => {
  const bundler = webpack(config[0]);
  browserSync({
    proxy: {
      target: 'localhost:5000',
      middleware: [
        webpackDevMiddleware(bundler, {
          publicPath: '/',
          stats: config[0].stats
        }),
        webpackHotMiddleware(bundler)
      ]
    },
    files: ['dist/public/**/*.css', 'dist/public/**/*.html']
  });
});

gulp.task('styles', () => {
  gulp
    .src(paths.srcCss)
    .pipe(sourcemaps.init())
    .pipe(postcss([vars, extend, nested, autoprefixer, cssnano]))
    .pipe(sourcemaps.write('.'))
    .pipe(gulp.dest(paths.dist))
    .pipe(reload({ stream: true }));
});

gulp.task('public', () => {
  gulp.src(paths.srcPublic).pipe(gulp.dest(paths.dist));
});

gulp.task('fonts', () => {
  gulp.src(paths.srcFonts).pipe(gulp.dest(`${paths.dist}/fonts`));
});

gulp.task('images', () => {
  gulp.src(paths.srcImg).pipe(gulp.dest(`${paths.dist}/images`));
});

// There are too many linting error. this needs to be disabled
// for now. It is causing watch to fail.
// After we fix all of the linting errors, we can enable it.
gulp.task('lint', () => {
  gulp
    .src(paths.srcLint)
    .pipe(eslint())
    .pipe(eslint.format());
});

gulp.task('watchTask', () => {
  gulp.watch(paths.srcFonts, ['fonts']);
  gulp.watch(paths.srcCss, ['styles']);
  gulp.watch(paths.srcPublic, ['public']);
  paths.srcLint.forEach(src => {
    gulp.watch(src, ['lint']);
  });
});

gulp.task('watch', cb => {
  runSequence('clean', ['browserSync', 'watchTask', 'public', 'styles', 'fonts', 'images'], cb);
});
