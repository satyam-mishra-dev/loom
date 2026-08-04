import * as Dialog from '@radix-ui/react-dialog';
import { X } from 'lucide-react';
import type { ReactElement, ReactNode } from 'react';
import { cn } from '../../lib/utils.js';

/* Sheet (right slide-over) and Dialog (centered) share Radix Dialog underneath.
   Themed to §3.2: translucent panel, backdrop blur, amber focus. */

const overlayClass =
  'fixed inset-0 z-50 bg-ground/70 backdrop-blur-sm data-[state=open]:animate-in data-[state=closed]:animate-out';

export function Sheet(props: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  children: ReactNode;
}): ReactElement {
  return (
    <Dialog.Root open={props.open} onOpenChange={props.onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className={overlayClass} />
        <Dialog.Content
          className={cn(
            'panel fixed right-0 top-0 z-50 flex h-full w-full max-w-[440px] flex-col rounded-none border-l',
            'transition-transform duration-200 data-[state=closed]:translate-x-full',
            'p-0',
          )}
        >
          <div className="flex items-center justify-between border-b border-line px-5 py-4">
            <Dialog.Title className="font-hud text-[15px] font-700 uppercase tracking-wide text-fg">
              {props.title}
            </Dialog.Title>
            <Dialog.Close
              aria-label="Close"
              className="rounded-md p-1 text-muted transition-colors hover:bg-fg/8 hover:text-fg"
            >
              <X size={18} />
            </Dialog.Close>
          </div>
          <div className="flex-1 overflow-y-auto px-5 py-4">{props.children}</div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

export function Modal(props: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  children: ReactNode;
}): ReactElement {
  return (
    <Dialog.Root open={props.open} onOpenChange={props.onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className={overlayClass} />
        <Dialog.Content
          className={cn(
            'panel fixed left-1/2 top-1/2 z-50 w-[calc(100vw-2rem)] max-w-[420px]',
            '-translate-x-1/2 -translate-y-1/2 p-5',
          )}
        >
          <Dialog.Title className="font-hud text-[16px] font-700 uppercase tracking-wide text-fg">
            {props.title}
          </Dialog.Title>
          {props.children}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
