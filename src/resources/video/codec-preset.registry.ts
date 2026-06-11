import { Injectable } from '@nestjs/common';

/**
 * Central source of the SVT-AV1 base parameter tables. Returns a fresh copy
 * per call so callers can append run-specific params without mutating the
 * shared table. An unknown preset name falls back to `main`.
 */
@Injectable()
export class CodecPresetRegistry {
  private readonly svtAV1PresetParams: Record<string, string[]> = {
    main: [
      'tune=0',
      'enable-overlays=1',
      'film-grain=0',
      'film-grain-denoise=0',
      'scd=1',
      'sharpness=0',
      'enable-qm=1',
      'qm-min=0',
      'enable-variance-boost=1'
    ],
    psy: ['tune=0', 'enable-overlays=1', 'film-grain=0', 'film-grain-denoise=0', 'sharpness=0', 'scd=1'],
    hdr: ['sharpness=0']
  };

  getSvtAv1BaseParams(preset: string): string[] {
    const table =
      preset === 'psy' ? this.svtAV1PresetParams.psy : preset === 'hdr' ? this.svtAV1PresetParams.hdr : this.svtAV1PresetParams.main;
    return [...table];
  }
}
