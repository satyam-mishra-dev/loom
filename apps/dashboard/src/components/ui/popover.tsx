import * as PopoverPrimitive from '@radix-ui/react-popover';
import type { ReactElement, ReactNode } from 'react';
import { cn } from '../../lib/utils.js';

export function Popover(props: {
  trigger: ReactNode;
  children: ReactNode;
  label: string;
}): ReactElement {
  return (
    <PopoverPrimitive.Root>
      <PopoverPrimitive.Trigger asChild aria-label={props.label}>
        {props.trigger}
      </PopoverPrimitive.Trigger>
      <PopoverPrimitive.Portal>
        <PopoverPrimitive.Content
          sideOffset={8}
          collisionPadding={12}
          className={cn(
            'panel z-50 w-[300px] max-w-[calc(100vw-2rem)] p-4 text-[13px] leading-relaxed text-fg/90',
            'data-[state=open]:animate-in data-[state=closed]:animate-out',
          )}
        >
          {props.children}
          <PopoverPrimitive.Arrow className="fill-[#223049]" />
        </PopoverPrimitive.Content>
      </PopoverPrimitive.Portal>
    </PopoverPrimitive.Root>
  );
}
