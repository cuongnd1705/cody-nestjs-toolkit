import { CanActivate, ExecutionContext, Inject, InjectionToken, Type, mixin } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { Observable, OperatorFunction, defer, from, of, throwError } from 'rxjs';
import { catchError, concatMap, defaultIfEmpty, filter, take } from 'rxjs/operators';

interface SequentialGuardOptions {
  throwOnError?: boolean;
}

export const SequentialGuard = (
  guards: Array<Type<CanActivate> | InjectionToken>,
  sequentialGuardOptions?: SequentialGuardOptions,
) => {
  class SequentialMixinGuard implements CanActivate {
    private guards: CanActivate[] = [];

    constructor(
      @Inject(ModuleRef)
      private readonly moduleRef: ModuleRef,
    ) {}

    canActivate(context: ExecutionContext): Observable<boolean> {
      this.guards = guards.map((guard) => this.moduleRef.get(guard, { strict: false }));

      const canActivateReturns: Array<Observable<boolean>> = this.guards.map((guard) =>
        this.deferGuard(guard, context),
      );

      return from(canActivateReturns).pipe(
        concatMap((obs) => obs.pipe(this.handleError())),
        filter((value) => value === true),
        defaultIfEmpty(false),
        take(1),
      );
    }

    private deferGuard(guard: CanActivate, context: ExecutionContext): Observable<boolean> {
      return defer(() => {
        const guardVal = guard.canActivate(context);

        if (this.guardIsPromise(guardVal)) {
          return from(guardVal);
        }

        if (this.guardIsObservable(guardVal)) {
          return guardVal;
        }

        return of(guardVal);
      });
    }

    private handleError(): OperatorFunction<boolean, boolean> {
      return catchError((err) => {
        if (sequentialGuardOptions?.throwOnError) {
          return throwError(() => err);
        }

        return of(false);
      });
    }

    private guardIsPromise(guard: boolean | Promise<boolean> | Observable<boolean>): guard is Promise<boolean> {
      return !!(guard as Promise<boolean>).then;
    }

    private guardIsObservable(guard: boolean | Observable<boolean>): guard is Observable<boolean> {
      return !!(guard as Observable<boolean>).pipe;
    }
  }

  const Guard = mixin(SequentialMixinGuard);

  return Guard as Type<CanActivate>;
};
