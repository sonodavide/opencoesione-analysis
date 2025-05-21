import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';

import { CommonModule } from '@angular/common';
import { TestHttpService } from './services/testHttp.service';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, CommonModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent {
  title = 'opencoesione-analysis-frontend';
  data: any;

  constructor(private testService: TestHttpService) { }

  ngOnInit(): void {
    this.testService.getPublicData().subscribe(response => {
      this.data = response;
      console.log('Dati ricevuti:', this.data);
    });
  }
}
