import { spawn } from 'child_process';

/**
 * The 64 loudness values (0-100) WhatsApp draws as a voice note's waveform. Without them phones
 * and WhatsApp Web show a flat line. Never throws: on any failure the note goes without one.
 */
export function voiceWaveform(audio: Buffer, bars = 64): Promise<Uint8Array | null> {
  return new Promise((resolve) => {
    let done = false;
    const finish = (value: Uint8Array | null) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      resolve(value);
    };
    const ff = spawn('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-i', 'pipe:0', '-f', 's16le', '-ac', '1', '-ar', '8000', 'pipe:1']);
    const timer = setTimeout(() => {
      ff.kill('SIGKILL');
      finish(null);
    }, 15000);
    const chunks: Buffer[] = [];
    ff.stdout.on('data', (c: Buffer) => chunks.push(c));
    ff.on('error', () => finish(null));
    ff.stdin.on('error', () => undefined);
    ff.on('close', (code) => {
      if (code !== 0) return finish(null);
      const pcm = Buffer.concat(chunks);
      const samples = Math.floor(pcm.length / 2);
      if (!samples) return finish(null);
      const per = Math.max(1, Math.floor(samples / bars));
      const levels: number[] = [];
      for (let b = 0; b < bars; b++) {
        let sum = 0;
        let n = 0;
        for (let i = b * per; i < Math.min(samples, (b + 1) * per); i++) {
          sum += Math.abs(pcm.readInt16LE(i * 2));
          n++;
        }
        levels.push(n ? sum / n : 0);
      }
      const top = Math.max(...levels, 1);
      finish(Uint8Array.from(levels.map((v) => Math.round((v / top) * 100))));
    });
    ff.stdin.end(audio);
  });
}
