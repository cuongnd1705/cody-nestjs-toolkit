import { CanActivate, ExecutionContext, Inject, InjectionToken, Type, mixin } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { Observable, OperatorFunction, defer, from, of, throwError } from 'rxjs';
import { catchError, last, mergeMap, takeWhile } from 'rxjs/operators';

interface OrGuardOptions {
  throwOnFirstError?: boolean;
}

export const OrGuard = (guards: Array<Type<CanActivate> | InjectionToken>, orGuardOptions?: OrGuardOptions) => {
  class OrMixinGuard implements CanActivate {
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
        mergeMap((obs) => obs.pipe(this.handleError())),
        takeWhile((val) => val === false, true),
        last(),
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
        if (orGuardOptions?.throwOnFirstError) {
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

  const Guard = mixin(OrMixinGuard);

  return Guard as Type<CanActivate>;
};
