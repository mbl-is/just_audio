import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:just_audio/just_audio.dart';

// These tests use real files and a real local HTTP server, as the failures
// they cover come from the file system: a cache directory cleared by the OS
// during a download, and a full disk. A failure that isn't handled shows up
// as an uncaught error, which fails the test, or as a request that never
// completes, which times out.

final data = List<int>.generate(1000, (i) => i % 256);

/// Serves [data], optionally stopping after the first 100 bytes of the first
/// request until [proceed] completes, then finishing or dropping the
/// connection.
class AudioServer {
  late HttpServer _server;
  int requests = 0;
  final proceed = Completer<void>();
  final bool pauseFirst;
  final bool dropFirst;

  AudioServer({this.pauseFirst = false, this.dropFirst = false});

  Uri get uri => Uri.parse(
      'http://${InternetAddress.loopbackIPv4.address}:${_server.port}/book.mp3');

  Future<void> start() async {
    _server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((request) async {
      final first = ++requests == 1;
      request.response
        ..contentLength = data.length
        ..headers.contentType = ContentType('audio', 'mpeg');
      final socket = await request.response.detachSocket();
      if (first && pauseFirst) {
        socket.add(data.sublist(0, 100));
        await socket.flush();
        await proceed.future;
        if (dropFirst) {
          socket.destroy();
          return;
        }
        socket.add(data.sublist(100));
      } else {
        socket.add(data);
      }
      await socket.close();
    });
  }

  Future<void> stop() => _server.close(force: true);
}

/// Collects a response stream, completing [firstData] when data arrives.
class Received {
  final bytes = <int>[];
  final firstData = Completer<void>();
  final done = Completer<void>();

  Received(Stream<List<int>> stream) {
    stream.listen((chunk) {
      bytes.addAll(chunk);
      if (!firstData.isCompleted) firstData.complete();
    }, onError: (Object e, StackTrace st) {
      if (!done.isCompleted) done.completeError(e, st);
    }, onDone: () {
      if (!done.isCompleted) done.complete();
    });
  }
}

Future<List<int>> readAll(Stream<List<int>> stream) =>
    stream.expand((chunk) => chunk).toList();

/// Gives the download's onDone/onError handlers time to finish after the
/// response streams they close.
Future<void> settle() => Future<void>.delayed(const Duration(milliseconds: 50));

void main() {
  // flutter_test replaces HttpClient with one that answers 400, unless
  // overridden like this
  HttpOverrides.global = _RealHttp();

  late Directory dir;
  late File cacheFile;
  late File partialFile;

  setUp(() {
    dir = Directory.systemTemp.createTempSync('lock_caching_audio_source');
    cacheFile = File('${dir.path}/book.mp3');
    partialFile = File('${cacheFile.path}.part');
  });

  tearDown(() => dir.deleteSync(recursive: true));

  test('caches the download and serves later requests from the cache',
      () async {
    final server = AudioServer();
    await server.start();
    final source = LockCachingAudioSource(server.uri, cacheFile: cacheFile);

    expect(await readAll((await source.request()).stream), data);
    await settle();
    expect(cacheFile.readAsBytesSync(), data);
    expect(partialFile.existsSync(), isFalse);

    expect(await readAll((await source.request()).stream), data);
    expect(server.requests, 1);
    await server.stop();
  });

  test('downloads again when the partial file disappears during the download',
      () async {
    final server = AudioServer(pauseFirst: true);
    await server.start();
    final source = LockCachingAudioSource(server.uri, cacheFile: cacheFile);

    final first = Received((await source.request()).stream);
    await first.firstData.future;
    // as when the OS clears the app's cache directory
    partialFile.deleteSync();
    server.proceed.complete();
    await first.done.future;
    expect(first.bytes, data);
    await settle();

    // the download must not be left marked as in progress
    await source.clearCache();
    expect(await readAll((await source.request()).stream), data);
    expect(server.requests, 2);
    await settle();
    expect(cacheFile.readAsBytesSync(), data);
    await server.stop();
  });

  test('fails the response and can download again when the connection drops '
      'after the partial file disappeared', () async {
    final server = AudioServer(pauseFirst: true, dropFirst: true);
    await server.start();
    final source = LockCachingAudioSource(server.uri, cacheFile: cacheFile);

    final first = Received((await source.request()).stream);
    await first.firstData.future;
    partialFile.deleteSync();
    server.proceed.complete();
    await expectLater(first.done.future, throwsA(isA<HttpException>()));
    await settle();

    await source.clearCache();
    expect(await readAll((await source.request()).stream), data);
    expect(server.requests, 2);
    await server.stop();
  });

  test('fails the response and deletes the partial file when the connection '
      'drops', () async {
    final server = AudioServer(pauseFirst: true, dropFirst: true);
    await server.start();
    final source = LockCachingAudioSource(server.uri, cacheFile: cacheFile);

    final first = Received((await source.request()).stream);
    await first.firstData.future;
    server.proceed.complete();
    await expectLater(first.done.future, throwsA(isA<HttpException>()));
    await settle();
    expect(partialFile.existsSync(), isFalse);

    expect(await readAll((await source.request()).stream), data);
    expect(server.requests, 2);
    await server.stop();
  });

  test('fails the request and can download again when the disk is full',
      () async {
    final server = AudioServer();
    await server.start();
    final source = LockCachingAudioSource(server.uri, cacheFile: cacheFile);
    // writes to /dev/full fail with "No space left on device"
    Link(partialFile.path).createSync('/dev/full');

    await expectLater(source.request(), throwsA(isA<FileSystemException>()));
    await settle();
    // as when space has been freed
    Link(partialFile.path).deleteSync();

    await source.clearCache();
    expect(await readAll((await source.request()).stream), data);
    expect(server.requests, 2);
    await server.stop();
  }, skip: File('/dev/full').existsSync() ? false : 'needs /dev/full');
}

class _RealHttp extends HttpOverrides {}
