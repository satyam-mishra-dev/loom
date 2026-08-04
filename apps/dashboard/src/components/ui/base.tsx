import { cva, type VariantProps } from 'class-variance-authority';
import type { ButtonHTMLAttributes, HTMLAttributes, ReactElement } from 'react';
import { cn } from '../../lib/utils.js';

/* shadcn-style primitives, themed hard to the design tokens. Copy-in components (the shadcn
   model), not a runtime library — every color is a token. */

const button = cva(
  'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md font-hud font-600 uppercase tracking-wide transition-colors duration-150 disabled:pointer-events-none disabled:opacity-40 focus-visible:outline-2 focus-visible:outline-amber',
  {
    variants: {
      variant: {
        primary: 'bg-amber/15 border border-amber/60 text-amber hover:bg-amber/25',
        solid: 'bg-amber text-ground border border-amber hover:bg-amber/90 font-700',
        alarm: 'bg-alarm/15 border border-alarm/60 text-alarm hover:bg-alarm/25',
        ghost: 'border border-line/70 text-fg/90 hover:bg-fg/5',
      },
      size: {
        sm: 'h-8 px-3 text-[12px]',
        md: 'h-9 px-4 text-[13px]',
      },
    },
    defaultVariants: { variant: 'ghost', size: 'md' },
  },
);

export interface ButtonProps
  extends ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof button> {}

export function Button({ className, variant, size, ...props }: ButtonProps): ReactElement {
  return <button className={cn(button({ variant, size }), className)} {...props} />;
}

export function Card({ className, ...props }: HTMLAttributes<HTMLDivElement>): ReactElement {
  return <div className={cn('panel text-fg', className)} {...props} />;
}

const badge = cva(
  'inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[11px] font-mono font-500 tnum',
  {
    variants: {
      tone: {
        neutral: 'bg-fg/8 text-muted border border-line',
        amber: 'bg-amber/12 text-amber border border-amber/40',
        intrip: 'bg-intrip/12 text-intrip border border-intrip/40',
        matched: 'bg-matched/12 text-matched border border-matched/40',
        alarm: 'bg-alarm/12 text-alarm border border-alarm/40',
      },
    },
    defaultVariants: { tone: 'neutral' },
  },
);

export interface BadgeProps extends HTMLAttributes<HTMLSpanElement>, VariantProps<typeof badge> {}

export function Badge({ className, tone, ...props }: BadgeProps): ReactElement {
  return <span className={cn(badge({ tone }), className)} {...props} />;
}

export function Skeleton({ className, ...props }: HTMLAttributes<HTMLDivElement>): ReactElement {
  return <div className={cn('animate-pulse rounded-md bg-fg/10', className)} {...props} />;
}
