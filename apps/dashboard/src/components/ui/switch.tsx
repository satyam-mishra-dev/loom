import * as SwitchPrimitive from '@radix-ui/react-switch';
import type { ReactElement } from 'react';
import { cn } from '../../lib/utils.js';

/** Themed toggle (§3.2): amber when on, quiet line when off. */
export function Switch(props: {
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  id?: string;
  color?: string;
}): ReactElement {
  return (
    <SwitchPrimitive.Root
      id={props.id}
      checked={props.checked}
      onCheckedChange={props.onCheckedChange}
      className={cn(
        'peer inline-flex h-[18px] w-8 shrink-0 cursor-pointer items-center rounded-full border transition-colors duration-150',
        'data-[state=unchecked]:border-line data-[state=unchecked]:bg-fg/8',
        'data-[state=checked]:border-amber/60 data-[state=checked]:bg-amber/25',
      )}
      style={props.checked && props.color !== undefined ? { borderColor: props.color, background: `${props.color}33` } : undefined}
    >
      <SwitchPrimitive.Thumb
        className="pointer-events-none block h-3 w-3 translate-x-[3px] rounded-full bg-muted shadow-sm transition-transform duration-150 data-[state=checked]:translate-x-[15px]"
        style={props.checked && props.color !== undefined ? { background: props.color } : undefined}
      />
    </SwitchPrimitive.Root>
  );
}
